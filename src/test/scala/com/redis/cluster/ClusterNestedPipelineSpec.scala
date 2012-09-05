package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import scala.{Some, Right}
import scala.Some

@RunWith(classOf[JUnitRunner])
class ClusterNestedPipelineSpec extends Spec
with ShouldMatchers {

  val r = new RedisCluster(new ConfigManager {
    def readConfig = Map("redis_6379" -> NodeConfig("localhost",6379))

    def addListener(listener: ClusterConfigListener) {}
  }) {
    val keyTag = Some(RegexKeyTag)
  }


  describe("pipeline") {
    it("should flatten results of all nested calls") {
      r.pipeline {
        p =>
          p.set("key1", "1")
          p.pipeline {
            p2 =>
              p2.set("key2", "2")
              p2.set("key3", "3")
          }
          p.set("key4", "4")
          p.get("key1")
          p.pipeline {
            p2 =>
              p2.get("key2")
              p2.get("key3")
          }
          p.get("key4")
      }.right.get should equal(List(
        Right(true),
        Right(true),
        Right(true),
        Right(true),
        Right(Some("1")),
        Right(Some("2")),
        Right(Some("3")),
        Right(Some("4"))
      ))
    }

    it("should prohibit getting results of nested pipeline") {
      r.set("key1", "1")
      r.set("key2", "2")

      var nestedRes: Either[Exception, Any] = null

      r.pipeline {
        p =>
          p.get("key1")
          nestedRes = p.pipeline {
            _.get("key2")
          }
      }

      nestedRes.isLeft should equal(true)
      nestedRes.left.get.isInstanceOf[IllegalStateException] should equal(true)

    }
  }

}
