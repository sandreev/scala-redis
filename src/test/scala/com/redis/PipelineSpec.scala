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


  describe("pipeline1") {
    it("should do pipelined commands") {
      r.pipeline {
        p =>
          p.set("key", "debasish")
          p.get("key")
          p.get("key1")
      }.right.get should equal(Right(true) :: Right(Some("debasish")) :: Right(None) :: Nil)
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
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some(3)), Right(Some(3)), Right(Some(List(Some("germany"), Some("italy"), Some("france"))))))
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
      object SomeAppException extends Exception

      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          throw SomeAppException
      }

      res.right.get should equal(List(Right(true), Left(SomeAppException)))
      res.left.toOption.isEmpty should equal(true)

      r.get("a") should equal(Some("abc"))
    }
  }

  describe("pipeline5") {
    it("should terminate on connection exception while sending commands ") {
      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          throw new RedisConnectionException("want to discard")

      }

      res.right.toOption.isEmpty should equal(true)
      res.left.get.isInstanceOf[RedisConnectionException] should equal(true)
    }
  }

  describe("pipeline6") {
    it("should continue reading results after non-io exception ") {
      r.flushdb
      r.reconnect

      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          p.rename("a", "a")
          p.set("b", "abcd")
      }

      res.right.toOption.isEmpty should equal(false)

      res.right.get.zip(List(
        (v: Either[Exception, Any]) => v.right.get should equal(true),
        (v: Either[Exception, Any]) => v.left.get.isInstanceOf[RedisBusinessException] should equal(true),
        (v: Either[Exception, Any]) => v.right.get should equal(true)
      )).foreach {
        case (v, pred) => pred(v)
      }


    }
  }

}