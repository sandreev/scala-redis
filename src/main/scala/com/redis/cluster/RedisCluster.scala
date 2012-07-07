package com.redis.cluster


import com.redis._

import serialization._
import java.util.concurrent.{TimeUnit, ConcurrentLinkedQueue, Executors}
import org.slf4j.LoggerFactory
import com.redis.PipelineBuffer
import java.util
import collection.mutable.ListBuffer

/**
 * Consistent hashing distributes keys across multiple servers. But there are situations
 * like <i>sorting</i> or computing <i>set intersections</i> or operations like <tt>rpoplpush</tt>
 * in redis that require all keys to be collocated on the same server.
 * <p/>
 * One of the techniques that redis encourages for such forced key locality is called
 * <i>key tagging</i>. See <http://code.google.com/p/redis/wiki/FAQ> for reference.
 * <p/>
 * The trait <tt>KeyTag</tt> defines a method <tt>tag</tt> that takes a key and returns
 * the part of the key on which we hash to determine the server on which it will be located.
 * If it returns <tt>None</tt> then we hash on the whole key, otherwise we hash only on the
 * returned part.
 * <p/>
 * redis-rb implements a regex based trick to achieve key-tagging. Here is the technique
 * explained in redis FAQ:
 * <i>
 * A key tag is a special pattern inside a key that, if preset, is the only part of the key 
 * hashed in order to select the server for this key. For example in order to hash the key 
 * "foo" I simply perform the CRC32 checksum of the whole string, but if this key has a 
 * pattern in the form of the characters {...} I only hash this substring. So for example 
 * for the key "foo{bared}" the key hashing code will simply perform the CRC32 of "bared". 
 * This way using key tags you can ensure that related keys will be stored on the same Redis
 * instance just using the same key tag for all this keys. Redis-rb already implements key tags.
 * </i>
 */
trait KeyTag {
  def tag(key: Seq[Byte]): Option[Seq[Byte]]
}


object RegexKeyTag extends KeyTag {

  val tagStart = '{'.toByte
  val tagEnd = '}'.toByte

  def tag(key: Seq[Byte]) = {
    val start = key.indexOf(tagStart) + 1
    if (start > 0) {
      val end = key.indexOf(tagEnd, start)
      if (end > -1) Some(key.slice(start, end)) else None
    } else None
  }
}

object NoOpKeyTag extends KeyTag {
  def tag(key: Seq[Byte]) = Some(key)
}

abstract class RedisCluster(configManager: ConfigManager)
  extends ClusterRedisCommand
  with Pipeline
  with NodeManager
  with PubSub {
  val port = 0
  val host = null


  val log = LoggerFactory.getLogger(getClass)

  // abstract val
  val keyTag: Option[KeyTag]

  // default in libmemcached
  val POINTS_PER_SERVER = 160 // default in libmemcached

  // instantiating a cluster will automatically connect participating nodes to the server
  val initialConfig = configManager.readConfig

  // the hash ring will instantiate with the nodes up and added
  val hr = new CopyOnWriteHashRing[RedisClientPool](
    initialConfig.map {
      case (name, hostPort) =>
        log.info("Connecting to redis node " + name + "(" + hostPort + ")")
        (name, new RedisClientPool(hostPort.host, hostPort.port))
    },
    POINTS_PER_SERVER)


  object DisconnectManager {
    val DisconnectTimeoutMs = 120000
    val queue = new ConcurrentLinkedQueue[(Long, RedisClientPool)]
    val disconnectService = Executors.newSingleThreadScheduledExecutor()
    disconnectService.scheduleAtFixedRate(
      new Runnable {
        def run() {
          val curTime = System.currentTimeMillis()
          for (i <- 1 to queue.size()) {
            queue.remove() match {
              case (time, client) if (curTime - time >= DisconnectTimeoutMs) =>
                log.info("Closing redis client " + client)
                client.close
              case v => queue.add(v)
            }
          }
        }
      },
      30, 30, TimeUnit.SECONDS)

    def submit(client: RedisClientPool) {
      queue.add((System.currentTimeMillis(), client))
    }
  }

  configManager.addListener(new ClusterConfigListener {
    var prevConfig = initialConfig

    def configUpdated(newConfig: Map[String, NodeConfig]) {
      val diff = ClusterConfigDiff(prevConfig, newConfig)

      diff.updatedNodes.foreach {
        case (name, nodeConf) => {
          log.info("Changing node endpoint: " + name + " moved to " + nodeConf)
          val oldNode = hr.udpateNode(name, new RedisClientPool(nodeConf.host, nodeConf.port))
          DisconnectManager.submit(oldNode)
        }
      }

      diff.newNodes.foreach {
        case (name, nodeConf) => log.error("Attempt to add a node to cluster: " + name + ", " + nodeConf)
      }

      diff.deletedNodes.foreach {
        case (name, nodeConf) => log.error("Attempt to remove a node from cluster: " + name + ", " + nodeConf)
      }

    }
  })

  private[redis] def poolForKey(hr: HashRing[RedisClientPool],  key: Any)(implicit format: Format): RedisClientPool = {
    val bKey = format(key)
    hr.getNode(keyTag.flatMap(_.tag(bKey)).getOrElse(bKey))
  }



  def onAllConns[T](body: RedisClient => T) =
    hr.cluster.values.map(_.withClient (body))

  def close() {
    hr.cluster.values.foreach(_.close)
  }

  private def poolForKeys(hr: HashRing[RedisClientPool], keys: Any*) = {
    require(!keys.isEmpty,"Cannot determine node for empty keys set")
    val nodes = keys.toList.map(poolForKey(hr, _))
    nodes.forall(_ eq nodes.head) match {
      case true => nodes.head // all nodes equal
      case _ =>
        throw new UnsupportedOperationException("can only occur if all keys map to same node")
    }
  }


  def inSameNode[T](keys: Any*)(body: RedisCommand => T)(implicit format: Format): T =
    poolForKeys(hr.ringRef.get, keys: _*).withClient(body)

  def withNode[T](key: Any)(body: (RedisCommand) => T)(implicit format: Format): T =
    poolForKeys(hr.ringRef.get, key).withClient(body)

  def groupByNodes[T](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(implicit format: Format) = {
    val ring = hr.ringRef.get
    val keyList = key :: keys.toList
    keyList.groupBy(key => ring.getNode(keyTag.flatMap(_.tag(format(key))).getOrElse(format(key)))).toSeq.map {
      case (pool, keys) =>
        pool.withClient(body(_, keys))
    }
  }

  /* Pipeline */

  private class MultiNodePipeline(parent: RedisCluster) extends ClusterRedisCommand with NodeManager {
    val host = parent.host
    val port = parent.port
    val hr: HashRing[RedisClientPool] = parent.hr.ringRef.get()

    case class PipelineEntry(client: RedisClient, pipeline: PipelineBuffer, opIdxs: ListBuffer[Int])

    val borrowedClients = collection.JavaConversions.mapAsScalaMap(new util.IdentityHashMap[RedisClientPool, PipelineEntry])
    var operationIdx = 0

    private def pipelineNode(keys: Any*): RedisCommand = {
      val pool = parent.poolForKeys(hr, keys: _*)
      borrowedClients.get(pool) match {
        case Some(PipelineEntry(_, pipe, ops)) =>
          ops += operationIdx
          operationIdx += 1
          pipe
        case None =>
          val client = pool.pool.borrowObject().asInstanceOf[RedisClient]
          val pipeline = client.pipelineBuffer
          borrowedClients += (pool -> PipelineEntry(client, pipeline, ListBuffer.empty[Int]))
          pipelineNode(keys: _*)
      }
    }

    def inSameNode[T](keys: Any*)(body: (RedisCommand) => T)(implicit format: Format): T =
      body(pipelineNode(keys: _*))

    def withNode[T](key: Any)(body: (RedisCommand) => T)(implicit format: Format) =
      body(pipelineNode(key))

    def onAllConns[T](body: (RedisClient) => T) = throw new UnsupportedOperationException("Unsupported for cluster pipelineNode")

    def groupByNodes[T](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(implicit format: Format) = {
      val keyList = key :: keys.toList
      List(inSameNode(keyList: _*)(body(_, keyList)))
    }

    def flushAndGetResults(): List[Either[Exception, Any]] = {
      borrowedClients.toSeq.map {
        case (pool, PipelineEntry(client, pipe, indexes)) =>
          val nodeResults = try {
            indexes zip pipe.flushAndGetResults()
          } catch {
            case e: Exception =>
              val errReport = Left(e)
              (indexes zip Array.fill(indexes.size)(errReport)).toList
          }


          pool.pool.returnObject(client)
          nodeResults
      }.flatten.sortBy(_._1).map(_._2).toList
    }
  }

  def pipeline(f: RedisCommand => Any) = {
    val pipe = new MultiNodePipeline(this)

    val ex = try {
      f(pipe)
      None
    } catch {
      case e: Exception => Some(e)
    }

    ex match {
      case Some(e @ RedisConnectionException(_)) => Left(e)
      case _ => Right(pipe.flushAndGetResults())
    }
  }
}
