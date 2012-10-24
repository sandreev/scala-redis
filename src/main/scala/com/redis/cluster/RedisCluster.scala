package com.redis.cluster


import com.redis._

import serialization._
import java.util.concurrent.{TimeUnit, ConcurrentLinkedQueue, Executors}
import org.slf4j.LoggerFactory
import com.redis.PipelineBuffer
import java.util
import collection.mutable.ListBuffer
import annotation.tailrec

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
  def tag(key: Array[Byte]): Option[Array[Byte]]
}


object RegexKeyTag extends KeyTag {

  val tagStart = '{'.toByte
  val tagEnd = '}'.toByte

  def tag(key: Array[Byte]) = {
    val start = key.indexOf(tagStart) + 1
    if (start > 0) {
      val end = key.indexOf(tagEnd, start)
      if (end > -1) {
        val slice = new Array[Byte](end - start)
        Array.copy(key, start, slice, 0, slice.length)
        Some(slice)
      } else
        None
    } else None
  }
}

object NoOpKeyTag extends KeyTag {
  def tag(key: Array[Byte]) = Some(key)
}

abstract class RedisCluster(configManager: ConfigManager)
  extends ClusterRedisCommand
  with Pipeline
  with Transactional
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

  private[redis] def poolForKey(hr: HashRing[RedisClientPool], key: Any)(implicit format: Format): RedisClientPool = {
    val bKey = format(key)
    hr.getNode(keyTag.flatMap(_.tag(bKey)).getOrElse(bKey))
  }


  def onAllConns[T, R](body: RedisClient => T)(merge: Iterable[T] => R) =
    Some(merge(hr.cluster.values.map(_.withClient(body))))

  def close() {
    hr.cluster.values.foreach(_.close)
  }

  private[cluster] def poolForKeys(hr: HashRing[RedisClientPool], keys: Any*) = {
    require(!keys.isEmpty, "Cannot determine node for empty keys set")
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

  def groupByNodes[T, R](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(merge: Iterable[(Seq[Any], T)] => R)(implicit format: Format) = {
    val ring = hr.ringRef.get
    val keyList = key :: keys.toList
    Some(merge(keyList.groupBy(key => ring.getNode(keyTag.flatMap(_.tag(format(key))).getOrElse(format(key)))).toSeq.map {
      case (pool, keys) =>
        (keys, pool.withClient(body(_, keys)))
    }))
  }

  /* Pipeline */

  private class MultiNodePipeline(parent: RedisCluster) extends ClusterRedisCommand with NodeManager with Pipeline {
    val host = parent.host
    val port = parent.port
    val hr: HashRing[RedisClientPool] = parent.hr.ringRef.get()

    case class PipelineEntry(client: RedisClient, pipeline: PipelineBuffer, opIdxs: ListBuffer[(Int, Seq[Any])])

    val borrowedClients = collection.JavaConversions.mapAsScalaMap(new util.IdentityHashMap[RedisClientPool, PipelineEntry])
    val mergers = collection.mutable.Map.empty[Int, Function[Iterable[(Seq[Any], Any)], Any]]
    var operationIdx = 0

    private def pipelineNode(keys: Any*): RedisCommand = {
      val res = pipelineNode(operationIdx, keys: _*)
      operationIdx += 1
      res
    }

    private def pipelineNode(idx: Int, keys: Any*): RedisCommand = {
      val pool = parent.poolForKeys(hr, keys: _*)
      borrowedClients.get(pool) match {
        case Some(PipelineEntry(_, pipe, ops)) =>
          /*ops.lastOption match {
            case Some((i, _)) if idx != i =>
              ops += ((idx, keys))
            case Some((i, prevKeys)) =>
              val pos = ops.size - 1
              ops(pos) = (idx, prevKeys ++ keys)
            case None =>
              ops += ((idx, keys))
          } */
          ops.headOption match {
            case Some((i, _)) if idx != i =>
              ((idx, keys)) +=: ops
            case Some((i, prevKeys)) =>
              ops(0) = (idx, prevKeys ++ keys)
            case None =>
               ((idx, keys)) +=: ops
          }
          pipe
        case None =>
          val client = pool.pool.borrowObject()
          val pipeline = client.pipelineBuffer
          borrowedClients += (pool -> PipelineEntry(client, pipeline, ListBuffer.empty[(Int, Seq[Any])]))
          pipelineNode(idx, keys: _*)
      }
    }

    def inSameNode[T](keys: Any*)(body: (RedisCommand) => T)(implicit format: Format): T =
      body(pipelineNode(keys: _*))

    def withNode[T](key: Any)(body: (RedisCommand) => T)(implicit format: Format) =
      body(pipelineNode(key))

    def onAllConns[T, R](body: (RedisClient) => T)(merge: Iterable[T] => R) = throw new UnsupportedOperationException("Unsupported for cluster pipelineNode")

    def groupByNodes[T, R](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(merge: Iterable[(Seq[Any], T)] => R)(implicit format: Format) = {
      val keyList = key :: keys.toList

      /*List(inSameNode(keyList: _*)(body(_, keyList)))*/
      keyList.groupBy(key => pipelineNode(operationIdx, key)).toSeq.foreach {
        case (pipe, keys) =>
          body(pipe, keys)
      }
      mergers += operationIdx -> merge.asInstanceOf[Function[Iterable[(Seq[Any], Any)], Any]]
      operationIdx += 1
      None
    }

    def flushAndGetResults(): List[Either[Exception, Any]] = {
      borrowedClients.foreach {
        case (pool, PipelineEntry(_, pipe, _)) =>
          try {
            pipe.flush()
          } catch {
            case e: Exception => log.error("Error flushing pipe in node " + pool, e)
          }
      }


      //val arr = new Array[List[Either[Exception, Any]]](this.operationIdx)
      //val arr = Array.fill(this.operationIdx)(List.empty[Either[Exception, Any]])
      val arr = collection.mutable.Map.empty[Int, List[(Seq[Any], Either[Exception, Any])]].withDefaultValue(Nil)

      borrowedClients.foreach {
        case (pool, PipelineEntry(client, pipe, indexes)) =>
          var errorOccurred = false
          try {
            val iter = indexes.reverseIterator
            pipe.readResults().foreach {
              v =>
                val (idx, keys) = iter.next()
                //arr(iter.next()) ::= _
                arr += idx -> ((keys, v) :: arr(idx))
            }

          } catch {
            case e: Exception =>
              val errReport = Left(e)
              indexes.foreach {
                case (idx, keys) =>
                //arr(_) ::= errReport
                  arr += idx -> ((keys, errReport) :: arr(idx))
              }
              errorOccurred = true
          } finally {
            if (errorOccurred)
              pool.pool.invalidateObject(client)
            else
              pool.pool.returnObject(client)
          }
      }
      arr.toList.sortBy(_._1).map {
        case (idx, keysAndResults) if mergers.contains(idx) & keysAndResults.forall(_._2.isRight) =>
          try {
            Right(mergers(idx)(keysAndResults.map(arg => (arg._1, arg._2.right.get))))
          } catch {
            case e: Exception => Left(e)
          }
        case (idx, keysAndResults) if keysAndResults.find(_._2.isLeft).isDefined =>
          keysAndResults.find(_._2.isLeft).get._2
        case (_, v :: Nil) => v._2
        case _ => throw new IllegalArgumentException("list of results without merger")
      }.toList
    }

    def pipeline(f: (RedisCommand with Pipeline) => Any) = {
      f(this)
      Left(new IllegalStateException("Results of nested sharded pipeline"))
    }
  }

  def pipeline(f: RedisCommand with Pipeline => Any) = {
    val pipe = new MultiNodePipeline(this)

    val ex = try {
      f(pipe)
      None
    } catch {
      case e: Exception => Some(e)
    }

    ex match {
      case Some(e: RedisConnectionException) => Left(e)
      case Some(e) => Right(pipe.flushAndGetResults() ::: List(Left(e)))
      case None => Right(pipe.flushAndGetResults())
    }
  }


  override def stmLike[T](precondition: (RedisCommand) => (Boolean, T))(action: (RedisCommand, T) => Any): Either[Exception, Option[List[Any]]] = {
    def functionalPrecondition(r: RedisCommand): Either[Exception, (Boolean, T)] = try {
      Right(precondition(r))
    } catch {
      case e: Exception =>
        Left(e)
    }

    @tailrec
    def checkAndAct: Either[Exception, Option[List[Any]]] = {
      val tx = new SingleNodeConstraint(this)
      functionalPrecondition(tx) match {
        case Right((true, v)) =>
          tx.transaction(action(_, v)) match {
            case Right(None) =>
              tx.close(None)
              checkAndAct
            case v@Right(Some(_)) =>
              tx.close(None)
              v
            case ex@Left(e) =>
              tx.close(Some(e))
              ex
          }
        case Right((false, _)) =>
          tx.close(None)
          Right(None)
        case Left(e) =>
          tx.close(Some(e))
          Left(e)
      }
    }

    checkAndAct
  }

  def transaction(f: RedisCommand => Any): Either[Exception, Option[List[Any]]] = new SingleNodeConstraint(this).transaction(f)
}
