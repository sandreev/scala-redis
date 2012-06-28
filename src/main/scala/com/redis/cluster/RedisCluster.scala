package com.redis.cluster


import com.redis._

import serialization._
import java.util.concurrent.{TimeUnit, ConcurrentLinkedQueue, Executors}
import org.slf4j.LoggerFactory
import com.redis.PipelineBuffer

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

  // get node for the key
  def nodeForKey(key: Any)(implicit format: Format) = {
    poolForKey(key)
  }

  private[redis] def poolForKey(hr: HashRing[RedisClientPool],  key: Any)(implicit format: Format) = {
    val bKey = format(key)
    hr.getNode(keyTag.flatMap(_.tag(bKey)).getOrElse(bKey))
  }

  private[redis] def poolForKey(key: Any)(implicit format: Format) = {
    val bKey = format(key)
    hr.getNode(keyTag.flatMap(_.tag(bKey)).getOrElse(bKey))
  }



  def onAllConns[T](body: RedisClient => T) =
    hr.cluster.values.map(p => p.withClient {
      client => body(client)
    }) // .forall(_ == true)

  def close = hr.cluster.values.foreach(_.close)

  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = nodeForKey(oldkey).withClient(_.rename(oldkey, newkey))

  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = nodeForKey(oldkey).withClient(_.renamenx(oldkey, newkey))

  override def dbsize: Option[Int] =
    Some(onAllConns(_.dbsize).foldLeft(0)((a, b) => b.map(a +).getOrElse(a)))

  override def exists(key: Any)(implicit format: Format): Boolean = nodeForKey(key).withClient(_.exists(key))

  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Int] =
    Some((key :: keys.toList).groupBy(nodeForKey).foldLeft(0) {
      case (t, (n, ks)) => n.withClient(_.del(ks.head, ks.tail: _*)).map(t +).getOrElse(t)
    })

  override def getType(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.getType(key))

  override def expire(key: Any, expiry: Int)(implicit format: Format) = nodeForKey(key).withClient(_.expire(key, expiry))

  override def expireAt(key: Any, expireAt: Int)(implicit format: Format) = nodeForKey(key).withClient(_.expireAt(key, expireAt))

  override def select(index: Int) = throw new UnsupportedOperationException("not supported on a cluster")

  /**
   * NodeOperations
   */
  override def save = onAllConns(_.save) forall (_ == true)

  override def bgsave = onAllConns(_.bgsave) forall (_ == true)

  override def shutdown = onAllConns(_.shutdown) forall (_ == true)

  override def bgrewriteaof = onAllConns(_.bgrewriteaof) forall (_ == true)

  override def lastsave = throw new UnsupportedOperationException("not supported on a cluster")

  override def monitor = throw new UnsupportedOperationException("not supported on a cluster")

  override def info = throw new UnsupportedOperationException("not supported on a cluster")

  override def slaveof(options: Any) = throw new UnsupportedOperationException("not supported on a cluster")

  override def move(key: Any, db: Int)(implicit format: Format) = throw new UnsupportedOperationException("not supported on a cluster")

  override def auth(secret: Any)(implicit format: Format) = throw new UnsupportedOperationException("not supported on a cluster")


  /**
   * StringOperations
   */
  override def set(key: Any, value: Any)(implicit format: Format) = nodeForKey(key).withClient(_.set(key, value))

  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.get(key))

  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.getset(key, value))

  override def setnx(key: Any, value: Any)(implicit format: Format) = nodeForKey(key).withClient(_.setnx(key, value))

  override def incr(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.incr(key))

  override def incrby(key: Any, increment: Int)(implicit format: Format) = nodeForKey(key).withClient(_.incrby(key, increment))

  override def decr(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.decr(key))

  override def decrby(key: Any, increment: Int)(implicit format: Format) = nodeForKey(key).withClient(_.decrby(key, increment))

  override def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    val keylist = (key :: keys.toList)
    val kvs = for {
      (n, ks) <- keylist.groupBy(nodeForKey)
      vs <- n.withClient(_.mget[A](ks.head, ks.tail: _*).toList)
      kv <- (ks).zip(vs)
    } yield kv
    Some(keylist.map(kvs))
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
    poolForKey(keys).withClient(body)

  /**
   * ListOperations
   */
  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format) = nodeForKey(key).withClient(_.lpush(key, value, values: _*))

  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format) = nodeForKey(key).withClient(_.lpush(key, value, values: _*))

  override def llen(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.llen(key))

  override def lrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.lrange[A](key, start, end))

  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format) = nodeForKey(key).withClient(_.ltrim(key, start, end))

  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.lindex(key, index))

  override def lset(key: Any, index: Int, value: Any)(implicit format: Format) = nodeForKey(key).withClient(_.lset(key, index, value))

  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format) = nodeForKey(key).withClient(_.lrem(key, count, value))

  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.lpop[A](key))

  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.rpop[A](key))

  override def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]) =
    inSameNode(srcKey, dstKey)(_.rpoplpush[A](srcKey, dstKey))

  /**
   * SetOperations
   */
  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] = nodeForKey(key).withClient(_.sadd(key, value, values: _*))

  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] = nodeForKey(key).withClient(_.srem(key, value, values: _*))

  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.spop[A](key))

  override def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format) =
    inSameNode(sourceKey, destKey)(_.smove(sourceKey, destKey, value))

  override def scard(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.scard(key))

  override def sismember(key: Any, value: Any)(implicit format: Format) = nodeForKey(key).withClient(_.sismember(key, value))

  override def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) =
    inSameNode((key :: keys.toList): _*)(_.sinter[A](key, keys: _*))

  override def sinterstore(key: Any, keys: Any*)(implicit format: Format) =
    inSameNode((key :: keys.toList): _*)(_.sinterstore(key, keys: _*))

  override def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) =
    inSameNode((key :: keys.toList): _*)(_.sunion[A](key, keys: _*))

  /* Pipeline */


  class SingleNodePipeline(parent: RedisCluster) extends ClusterRedisCommand with NodeManager {
    val host = parent.host
    val port = parent.port

    val hr: HashRing[RedisClientPool] = parent.hr.ringRef.get()

    var pool: RedisClientPool = _
    var borrowedClient: RedisClient = _
    var pipe: Option[PipelineBuffer] = None

    private def pipelineNode(keys: Any*): RedisCommand = pipe match {
      case Some(redis) =>
        if(!(parent.poolForKeys(hr, keys: _*) eq pool))
          throw new IllegalArgumentException("Pipeline operations for different nodes are not supported")
        redis
      case None =>
        pool = parent.poolForKeys(hr, keys: _*)
        borrowedClient = pool.pool.borrowObject().asInstanceOf[RedisClient]
        pipe = Some(borrowedClient.pipelineBuffer)
        pipelineNode(keys: _*)
    }

    def inSameNode[T](keys: Any*)(body: (RedisCommand) => T)(implicit format: Format): T =
      body(pipelineNode(keys: _*))


    def nodeForKey(key: Any)(implicit format: Format) = pipelineNode(key)


    def onAllConns[T](body: (RedisClient) => T) = throw new UnsupportedOperationException("Unsupported for cluster pipelineNode")

    def flushAndGetResults(): List[AsyncResult[Any]] = pipe match {
      case Some(redisPipe) =>
        try {
          redisPipe.flushAndGetResults()
        } finally {
          pool.pool.returnObject(borrowedClient)
        }
      case None => Nil
    }
  }

  override def sunionstore(key: Any, keys: Any*)(implicit format: Format) =
    inSameNode((key :: keys.toList): _*)(_.sunionstore(key, keys: _*))

  override def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) =
    inSameNode((key :: keys.toList): _*)(_.sdiff[A](key, keys: _*))

  override def sdiffstore(key: Any, keys: Any*)(implicit format: Format) =
    inSameNode((key :: keys.toList): _*)(_.sdiffstore(key, keys: _*))

  override def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.smembers(key))

  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.srandmember(key))


  import RedisClient._

  /**
   * SortedSetOperations
   */
  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format) =
    nodeForKey(key).withClient(_.zadd(key, score, member, scoreVals: _*))

  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Int] =
    nodeForKey(key).withClient(_.zrem(key, member, members))

  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format) = nodeForKey(key).withClient(_.zincrby(key, incr, member))

  override def zcard(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.zcard(key))

  override def zscore(key: Any, element: Any)(implicit format: Format) = nodeForKey(key).withClient(_.zscore(key, element))

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder)(implicit format: Format, parse: Parse[A]) =
    nodeForKey(key).withClient(_.zrange[A](key, start, end, sortAs))

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) =
    nodeForKey(key).withClient(_.zrangeWithScore[A](key, start, end, sortAs))

  override def zrangebyscore[A](key: Any, min: Double = Double.NegativeInfinity, minInclusive: Boolean = true, max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true, limit: Option[(Int, Int)], sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) =
    nodeForKey(key).withClient(_.zrangebyscore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs))

  override def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format): Option[Int] =
    nodeForKey(key).withClient(_.zcount(key, min, max, minInclusive, maxInclusive))

  /**
   * HashOperations
   */
  override def hset(key: Any, field: Any, value: Any)(implicit format: Format) = nodeForKey(key).withClient(_.hset(key, field, value))

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.hget[A](key, field))

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format) = nodeForKey(key).withClient(_.hmset(key, map))

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]) = nodeForKey(key).withClient(_.hmget[K, V](key, fields: _*))

  override def hincrby(key: Any, field: Any, value: Int)(implicit format: Format) = nodeForKey(key).withClient(_.hincrby(key, field, value))

  override def hexists(key: Any, field: Any)(implicit format: Format) = nodeForKey(key).withClient(_.hexists(key, field))

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Int] = nodeForKey(key).withClient(_.hdel(key, field, fields: _*))

  override def hlen(key: Any)(implicit format: Format) = nodeForKey(key).withClient(_.hlen(key))

  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.hkeys[A](key))

  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).withClient(_.hvals[A](key))

  override def hgetall[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]) = nodeForKey(key).withClient(_.hgetall[K, V](key))

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = nodeForKey(key).withClient(_.hsetnx(key, field, value))

  def pipeline(f: RedisCommand => Any) = {
    val pipe = new SingleNodePipeline(this)
    var error: Option[_ <: Exception] = None

    try {
      f(pipe)
    } catch {
      case e: Exception => error = Some(e)
    }

    error match {
      case Some(RedisConnectionException(_)) => (Nil, error)
      case _ => (pipe.flushAndGetResults(), error)
    }

  }
}
