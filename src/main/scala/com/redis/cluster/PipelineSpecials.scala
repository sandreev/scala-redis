package com.redis.cluster

import com.redis.serialization.{Parse, Format}
import com.redis.RedisCommand

trait PipelineSpecials extends ClusterRedisCommand {
  self: NodeManager =>

  override def del(key: Any, keys: Any*)(implicit format: Format) = {
    groupByNodes(key, keys: _*) {
      (client: RedisCommand, keys: Seq[Any]) =>
        client.del(keys.head, keys.tail: _*)
    }
    null
  }

  override def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) = {
    groupByNodes(key, keys: _*) {
      (client: RedisCommand, keys: Seq[Any]) =>
        client.mget(keys.head, keys.tail: _*).map(keys.zip(_)).getOrElse(List.empty[(Any, Option[A])])
    }
    null
  }

}
