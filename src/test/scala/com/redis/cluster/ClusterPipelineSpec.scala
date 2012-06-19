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
      }._1 should equal(results(true, Some("debasish"), Some("debasish")))
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
      }._1 should equal(results(Some(1), Some(2), Some(3), Some(List(Some("germany"), Some("italy"), Some("france")))))
    }
  }

  describe("pipeline3") {
    it("should handle errors properly in pipelined commands") {

      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          p.lpop("a")
      }

      res._1.head should equal(Success(true))
      res._1.tail.head.isInstanceOf[ExecError] should equal(true)
      res._2.isEmpty should equal(true)

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

      res._1 should equal(results(true))
      res._2.get.isInstanceOf[Exception] should equal(true)

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

      res._1.isEmpty should equal(true)
      res._2.get.isInstanceOf[RedisConnectionException] should equal(true)
    }
  }

  describe("pipeline5") {
    it("should prohibit commands with keys mapping to different nodes") {
      val res = r.pipeline {
        p =>
          for (i <- 1 to 100)
            p.set(i, i+1)
      }

      (res._1.size < 100) should equal(true)
      (res._2.isEmpty) should equal(false)
    }
  }



}
