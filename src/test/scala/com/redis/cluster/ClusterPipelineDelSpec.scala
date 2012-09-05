package com.redis.cluster

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ClusterPipelineDelSpec extends Spec
with ShouldMatchers with MockitoSugar{

  import org.mockito.Mockito._

  val conf = mock[ConfigManager]
  when(conf.readConfig).thenReturn(
    Map("1" -> NodeConfig("localhost", 6379)))

  val r = new RedisCluster(conf) {
    val keyTag = Some(RegexKeyTag)
  }

  describe("pipeline del") {
    it("should execute all dels") {
      r.pipeline {
        p =>
          p.set("key", "debasish")
          p.set("key1", "debasish1")
          p.set("key2", "debasish2")
          p.del("key")
          p.del("key1")
          p.del("key2")
          p.get("key")
          p.get("key1")
          p.get("key2")

      }.right.get should equal(Right(true) :: Right(true) :: Right(true) ::
        Right(Some(1)) :: Right(Some(1)) :: Right(Some(1)) ::
        Right(None) ::Right(None) ::Right(None) :: Nil)
    }
  }

}
