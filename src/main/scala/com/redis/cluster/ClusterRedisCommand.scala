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
      onAllConns( _.keys[A](pattern)).flatten.flatten.toList
    )

  override def flushdb = onAllConns(_.flushdb) forall (_ == true)

  override def flushall = onAllConns(_.flushall) forall (_ == true)

  override def quit = onAllConns(_.quit) forall (_ == true)

  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = nodeForKey(oldkey).rename(oldkey, newkey)

  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = nodeForKey(oldkey).renamenx(oldkey, newkey)

  override def dbsize: Option[Int] =
    Some(onAllConns(_.dbsize).foldLeft(0)((a, b) => b.map(a +).getOrElse(a)))

  override def exists(key: Any)(implicit format: Format): Boolean = nodeForKey(key).exists(key)

  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Int] =
    Some((key :: keys.toList).groupBy(nodeForKey).foldLeft(0) {
      case (t, (n, ks)) => n.del(ks.head, ks.tail: _*).map(t +).getOrElse(t)
    })

  override def getType(key: Any)(implicit format: Format) = nodeForKey(key).getType(key)

  override def expire(key: Any, expiry: Int)(implicit format: Format) = nodeForKey(key).expire(key, expiry)

  override def expireAt(key: Any, expireAt: Int)(implicit format: Format) = nodeForKey(key).expireAt(key, expireAt)

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
  override def set(key: Any, value: Any)(implicit format: Format) = nodeForKey(key).set(key, value)

  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).get(key)

  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).getset(key, value)

  override def setnx(key: Any, value: Any)(implicit format: Format) = nodeForKey(key).setnx(key, value)

  override def incr(key: Any)(implicit format: Format) = nodeForKey(key).incr(key)

  override def incrby(key: Any, increment: Int)(implicit format: Format) = nodeForKey(key).incrby(key, increment)

  override def decr(key: Any)(implicit format: Format) = nodeForKey(key).decr(key)

  override def decrby(key: Any, increment: Int)(implicit format: Format) = nodeForKey(key).decrby(key, increment)

  override def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    val keylist = (key :: keys.toList)
    val kvs = for {
      (n, ks) <- keylist.groupBy(nodeForKey)
      vs <- n.mget[A](ks.head, ks.tail: _*).toList
      kv <- (ks).zip(vs)
    } yield kv
    Some(keylist.map(kvs))
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
  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format) = nodeForKey(key).lpush(key, value, values: _*)

  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format) = nodeForKey(key).lpush(key, value, values: _*)

  override def llen(key: Any)(implicit format: Format) = nodeForKey(key).llen(key)

  override def lrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).lrange[A](key, start, end)

  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format) = nodeForKey(key).ltrim(key, start, end)

  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).lindex(key, index)

  override def lset(key: Any, index: Int, value: Any)(implicit format: Format) = nodeForKey(key).lset(key, index, value)

  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format) = nodeForKey(key).lrem(key, count, value)

  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).lpop[A](key)

  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).rpop[A](key)

  override def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]) =
    inSameNode(srcKey, dstKey) {
      n => n.rpoplpush[A](srcKey, dstKey)
    }

  /**
   * SetOperations
   */
  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] = nodeForKey(key).sadd(key, value, values: _*)

  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] = nodeForKey(key).srem(key, value, values: _*)

  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).spop[A](key)

  override def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format) =
    inSameNode(sourceKey, destKey) {
      n => n.smove(sourceKey, destKey, value)
    }

  override def scard(key: Any)(implicit format: Format) = nodeForKey(key).scard(key)

  override def sismember(key: Any, value: Any)(implicit format: Format) = nodeForKey(key).sismember(key, value)

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

  override def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).smembers(key)

  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).srandmember(key)


  import RedisClient._

  /**
   * SortedSetOperations
   */
  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format) =
    nodeForKey(key).zadd(key, score, member, scoreVals: _*)

  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Int] =
    nodeForKey(key).zrem(key, member, members)

  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format) = nodeForKey(key).zincrby(key, incr, member)

  override def zcard(key: Any)(implicit format: Format) = nodeForKey(key).zcard(key)

  override def zscore(key: Any, element: Any)(implicit format: Format) = nodeForKey(key).zscore(key, element)

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder)(implicit format: Format, parse: Parse[A]) =
    nodeForKey(key).zrange[A](key, start, end, sortAs)

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) =
    nodeForKey(key).zrangeWithScore[A](key, start, end, sortAs)

  override def zrangebyscore[A](key: Any, min: Double = Double.NegativeInfinity, minInclusive: Boolean = true, max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true, limit: Option[(Int, Int)], sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) =
    nodeForKey(key).zrangebyscore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs)

  override def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format): Option[Int] =
    nodeForKey(key).zcount(key, min, max, minInclusive, maxInclusive)

  /**
   * HashOperations
   */
  override def hset(key: Any, field: Any, value: Any)(implicit format: Format) = nodeForKey(key).hset(key, field, value)

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).hget[A](key, field)

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format) = nodeForKey(key).hmset(key, map)

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]) = nodeForKey(key).hmget[K, V](key, fields: _*)

  override def hincrby(key: Any, field: Any, value: Int)(implicit format: Format) = nodeForKey(key).hincrby(key, field, value)

  override def hexists(key: Any, field: Any)(implicit format: Format) = nodeForKey(key).hexists(key, field)

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Int] = nodeForKey(key).hdel(key, field, fields: _*)

  override def hlen(key: Any)(implicit format: Format) = nodeForKey(key).hlen(key)

  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).hkeys[A](key)

  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]) = nodeForKey(key).hvals[A](key)

  override def hgetall[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]) = nodeForKey(key).hgetall[K, V](key)

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = nodeForKey(key).hsetnx(key, field, value)


}
