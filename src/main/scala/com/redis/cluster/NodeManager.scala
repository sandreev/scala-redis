package com.redis.cluster

import com.redis.{RedisClientPool, Pipeline, RedisCommand, RedisClient}
import com.redis.serialization.Format

trait NodeManager {
  def inSameNode[T](keys: Any*)(body: RedisCommand => T)(implicit format: Format): T
  def nodeForKey(key: Any)(implicit format: Format): RedisCommand
  def onAllConns[T](body: RedisClient => T): Iterable[T]
}
