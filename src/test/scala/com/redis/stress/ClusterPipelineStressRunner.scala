package com.redis.stress

import com.redis.RedisCommand
import org.scalatest.mock.MockitoSugar
import com.redis.cluster._
import scala.Some
import com.redis.cluster.NodeConfig


object ClusterPipelineStressRunner extends App with MockitoSugar {

  import org.mockito.Mockito._

  val conf = mock[ConfigManager]
  when(conf.readConfig).thenReturn(
    Map("1" -> NodeConfig("localhost", 6379),
      "2" -> NodeConfig("localhost", 6380),
      "3" -> NodeConfig("localhost", 6381)))



  val r = new RedisCluster(conf) {
    val keyTag = Some(RegexKeyTag)
    override def isLookupLogEnabled(key: Any) = key.toString.contains("22")
  }

  abstract class Runner(val name: String) extends PipelineStress {

    def warmup() {
      onlyIncrs(r, 10000)
      r.pipeline(p => onlyIncrs(p, 10000))
    }

    def resetEnv() {
      r.flushdb
    }
  }

  new Runner("Simple") {
    def withRedis(f: (RedisCommand) => Any) {f(r)}
  }.runTest()

  new Runner("Pipeline") {
    def withRedis(f: (RedisCommand) => Any) {r.pipeline(f)}
  }.runTest()

}
