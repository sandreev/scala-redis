package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class PipelineSpec extends Spec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)

  override def beforeEach = {
  }

  override def afterEach = {
    r.flushdb
  }

  override def afterAll = {
    r.disconnect
  }

  def results(v: Any*) = v.map(Success(_)).toList

  describe("pipeline1") {
    it("should do pipelined commands") {
      r.pipeline {
        p =>
          p.set("key", "debasish")
          p.get("key")
          p.get("key1")
      }._1 should equal(results(true, Some("debasish"), None))
    }
  }

  describe("pipeline2") {
    it("should do pipelined commands") {
      r.pipeline {
        p =>
          p.lpush("country_list", "france")
          p.lpush("country_list", "italy")
          p.lpush("country_list", "germany")
          p.incrby("country_count", 3)
          p.lrange("country_list", 0, -1)
      }._1 should equal(results(Some(1), Some(2), Some(3), Some(3), Some(List(Some("germany"), Some("italy"), Some("france")))))
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

}
