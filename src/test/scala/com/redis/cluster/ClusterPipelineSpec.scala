package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import com.redis.{ExecError, RedisConnectionException, Success}

@RunWith(classOf[JUnitRunner])
class ClusterPipelineSpec extends Spec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with MockitoSugar {


  import org.mockito.Mockito._

  val conf = mock[ConfigManager]
  when(conf.readConfig).thenReturn(
    Map("1" -> NodeConfig("localhost", 6379),
      "2" -> NodeConfig("localhost", 6380),
      "3" -> NodeConfig("localhost", 6381)))

  val r = new RedisCluster(conf) {
    val keyTag = Some(RegexKeyTag)
  }

  override def beforeEach = {}

  override def afterEach = r.flushdb

  override def afterAll = r.close

  def results(v: Any*) = v.map(Success(_)).toList

  describe("pipeline1") {
    it("should do pipelined commands") {
      r.pipeline {
        p =>
          p.set("key", "debasish")
          p.get("key")
          p.get("key")
      }.right.get should equal(List(Right(true), Right(Some("debasish")), Right(Some("debasish"))))
    }
  }

  describe("pipeline2") {
    it("should do pipelined commands") {
      r.pipeline {
        p =>
          p.lpush("country_list", "france")
          p.lpush("country_list", "italy")
          p.lpush("country_list", "germany")
          p.lrange("country_list", 0, -1)
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some(3)), Right(Some(List(Some("germany"), Some("italy"), Some("france"))))))
    }
  }

  describe("pipeline3") {
    it("should handle errors properly in pipelined commands") {

      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          p.lpop("a")
      }

      res.right.get.head should equal(Right(true))
      res.right.get.tail.head.isInstanceOf[Left[Exception, Any]] should equal(true)
      res.left.toOption.isEmpty should equal(true)

      r.get("a").get should equal("abc")
    }
  }

  describe("pipeline4") {
    it("should execute commands before non-connection exception ") {
      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          throw new Exception("want to discard")
      }

      res.right.get should equal(List(Right(true)))
      res.left.toOption.isEmpty should equal(true)

      r.get("a") should equal(Some("abc"))
    }
  }

  describe("pipeline5") {
    it("should execute commands terminate on connection exception while sending commands ") {
      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          throw new RedisConnectionException("want to discard")
      }

      res.right.toOption.isEmpty should equal(true)
      res.left.get.isInstanceOf[RedisConnectionException] should equal(true)
    }
  }

  describe("pipeline5") {
    it("should prohibit commands with keys mapping to different nodes") {
      val res = r.pipeline {
        p =>
          for (i <- 1 to 100)
            p.set(i, i+1)
      }

      (res.right.get.size < 100) should equal(true)
    }
  }



}
