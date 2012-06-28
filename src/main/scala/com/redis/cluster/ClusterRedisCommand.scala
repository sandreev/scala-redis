package com.redis.cluster

import com.redis.{RedisClient, RedisCommand}
import com.redis.serialization.{Parse, Format}

trait ClusterRedisCommand extends RedisCommand {
  self: NodeManager =>
  /**
   * Operations
   */
  override def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]) =
    Some[List[Option[A]]](
      onAllConns(_.keys[A](pattern)).flatten.flatten.toList
    )

  override def flushdb = onAllConns(_.flushdb) forall (_ == true)

  override def flushall = onAllConns(_.flushall) forall (_ == true)

  override def quit = onAllConns(_.quit) forall (_ == true)

  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = inSameNode(oldkey, newkey)(_.rename(oldkey, newkey))

  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = withNode(oldkey)(_.renamenx(oldkey, newkey))

  override def dbsize: Option[Int] =
    Some(onAllConns(_.dbsize).foldLeft(0)((a, b) => b.map(a +).getOrElse(a)))

  override def exists(key: Any)(implicit format: Format): Boolean = withNode(key)(_.exists(key))

  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Int] =
    Some(
      groupByNodes(key, keys: _*) {
        (client: RedisCommand, keys: Seq[Any]) =>
          client.del(keys.head, keys.tail: _*)
      }.flatten.sum
    )


  override def getType(key: Any)(implicit format: Format) = withNode(key)(_.getType(key))

  override def expire(key: Any, expiry: Int)(implicit format: Format) = withNode(key)(_.expire(key, expiry))

  override def expireAt(key: Any, expireAt: Int)(implicit format: Format) = withNode(key)(_.expireAt(key, expireAt))

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
  override def set(key: Any, value: Any)(implicit format: Format) = withNode(key)(_.set(key, value))

  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.get(key))

  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.getset(key, value))

  override def setnx(key: Any, value: Any)(implicit format: Format) = withNode(key)(_.setnx(key, value))

  override def incr(key: Any)(implicit format: Format) = withNode(key)(_.incr(key))

  override def incrby(key: Any, increment: Int)(implicit format: Format) = withNode(key)(_.incrby(key, increment))

  override def decr(key: Any)(implicit format: Format) = withNode(key)(_.decr(key))

  override def decrby(key: Any, increment: Int)(implicit format: Format) = withNode(key)(_.decrby(key, increment))

  override def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    val keyList = key :: keys.toList
    val kvs = groupByNodes(key, keys: _*) {
      (client: RedisCommand, keys: Seq[Any]) =>
        client.mget(keys.head, keys.tail: _*).map(keys.zip(_)).getOrElse(List.empty[(Any, Option[A])])
    }.flatten.toMap
    Some(keyList.map(kvs).toList)

  }

  override def mset(kvs: (Any, Any)*)(implicit format: Format) = kvs.toList.map {
    case (k, v) => set(k, v)
  }.forall(_ == true)

  override def msetnx(kvs: (Any, Any)*)(implicit format: Format) = kvs.toList.map {
    case (k, v) => setnx(k, v)
  }.forall(_ == true)

  /**
   * ListOperations
   */
  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format) = withNode(key)(_.lpush(key, value, values: _*))

  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format) = withNode(key)(_.lpush(key, value, values: _*))

  override def llen(key: Any)(implicit format: Format) = withNode(key)(_.llen(key))

  override def lrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.lrange[A](key, start, end))

  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format) = withNode(key)(_.ltrim(key, start, end))

  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.lindex(key, index))

  override def lset(key: Any, index: Int, value: Any)(implicit format: Format) = withNode(key)(_.lset(key, index, value))

  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format) = withNode(key)(_.lrem(key, count, value))

  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.lpop[A](key))

  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.rpop[A](key))

  override def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]) =
    inSameNode(srcKey, dstKey) {
      n => n.rpoplpush[A](srcKey, dstKey)
    }

  /**
   * SetOperations
   */
  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] = withNode(key)(_.sadd(key, value, values: _*))

  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] = withNode(key)(_.srem(key, value, values: _*))

  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.spop[A](key))

  override def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format) =
    inSameNode(sourceKey, destKey) {
      n => n.smove(sourceKey, destKey, value)
    }

  override def scard(key: Any)(implicit format: Format) = withNode(key)(_.scard(key))

  override def sismember(key: Any, value: Any)(implicit format: Format) = withNode(key)(_.sismember(key, value))

  override def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) =
    inSameNode((key :: keys.toList): _*) {
      n => n.sinter[A](key, keys: _*)
    }

  override def sinterstore(key: Any, keys: Any*)(implicit format: Format) =
    inSameNode((key :: keys.toList): _*) {
      n => n.sinterstore(key, keys: _*)
    }

  override def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) =
    inSameNode((key :: keys.toList): _*) {
      n => n.sunion[A](key, keys: _*)
    }

  override def sunionstore(key: Any, keys: Any*)(implicit format: Format) =
    inSameNode((key :: keys.toList): _*) {
      n => n.sunionstore(key, keys: _*)
    }

  override def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) =
    inSameNode((key :: keys.toList): _*) {
      n => n.sdiff[A](key, keys: _*)
    }

  override def sdiffstore(key: Any, keys: Any*)(implicit format: Format) =
    inSameNode((key :: keys.toList): _*) {
      n => n.sdiffstore(key, keys: _*)
    }

  override def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.smembers(key))

  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.srandmember(key))


  import RedisClient._

  /**
   * SortedSetOperations
   */
  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format) =
    withNode(key)(_.zadd(key, score, member, scoreVals: _*))

  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Int] =
    withNode(key)(_.zrem(key, member, members))

  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format) = withNode(key)(_.zincrby(key, incr, member))

  override def zcard(key: Any)(implicit format: Format) = withNode(key)(_.zcard(key))

  override def zscore(key: Any, element: Any)(implicit format: Format) = withNode(key)(_.zscore(key, element))

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder)(implicit format: Format, parse: Parse[A]) =
    withNode(key)(_.zrange[A](key, start, end, sortAs))

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) =
    withNode(key)(_.zrangeWithScore[A](key, start, end, sortAs))

  override def zrangebyscore[A](key: Any, min: Double = Double.NegativeInfinity, minInclusive: Boolean = true, max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true, limit: Option[(Int, Int)], sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) =
    withNode(key)(_.zrangebyscore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs))

  override def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format): Option[Int] =
    withNode(key)(_.zcount(key, min, max, minInclusive, maxInclusive))

  /**
   * HashOperations
   */
  override def hset(key: Any, field: Any, value: Any)(implicit format: Format) = withNode(key)(_.hset(key, field, value))

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.hget[A](key, field))

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format) = withNode(key)(_.hmset(key, map))

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]) = withNode(key)(_.hmget[K, V](key, fields: _*))

  override def hincrby(key: Any, field: Any, value: Int)(implicit format: Format) = withNode(key)(_.hincrby(key, field, value))

  override def hexists(key: Any, field: Any)(implicit format: Format) = withNode(key)(_.hexists(key, field))

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Int] = withNode(key)(_.hdel(key, field, fields: _*))

  override def hlen(key: Any)(implicit format: Format) = withNode(key)(_.hlen(key))

  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.hkeys[A](key))

  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]) = withNode(key)(_.hvals[A](key))

  override def hgetall[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]) = withNode(key)(_.hgetall[K, V](key))

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = withNode(key)(_.hsetnx(key, field, value))


}
