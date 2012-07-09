package com.redis.stress

import com.redis.{RedisCommand, RedisClient}

object SimplePipelineStressRunner extends App {
  val redis = new RedisClient("localhost", 6379)


  abstract class Runner(val name: String) extends PipelineStress {
    def warmup() {
      onlyIncrs(redis, 10000)
      redis.pipeline(p => onlyIncrs(p, 10000))

    }

    def resetEnv() {
      redis.flushdb
    }
  }

  new Runner("Simple") {
    def withRedis(f: (RedisCommand) => Any) {f(redis)}
  }.runTest()

  new Runner("Pipeline") {
    def withRedis(f: (RedisCommand) => Any) {redis.pipeline(f)}
  }.runTest()
}
