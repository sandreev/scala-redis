package com.redis.cluster

import com.redis.{RedisClientPool, Pipeline, RedisCommand, RedisClient}
import com.redis.serialization.Format

trait NodeManager {
  def inSameNode[T](keys: Any*)(body: RedisCommand => T)(implicit format: Format): T

  def withNode[T](key: Any)(body: RedisCommand => T)(implicit format: Format): T

  def onAllConns[T, R](body: RedisClient => T)(merge: Iterable[T] => R): Option[R]

  def groupByNodes[T, R](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(merge: Iterable[(Seq[Any], T)] => R)(implicit format: Format): Option[R]

}
