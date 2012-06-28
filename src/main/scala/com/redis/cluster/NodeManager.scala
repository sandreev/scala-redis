package com.redis.cluster

import com.redis.{RedisClientPool, Pipeline, RedisCommand, RedisClient}
import com.redis.serialization.Format

trait NodeManager {
  def inSameNode[T](keys: Any*)(body: RedisCommand => T)(implicit format: Format): T
  def withNode[T](key: Any)(body: RedisCommand => T)(implicit format: Format): T
  def onAllConns[T](body: RedisClient => T): Iterable[T]
  def groupByNodes[T](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(implicit format: Format): Iterable[T]
}
