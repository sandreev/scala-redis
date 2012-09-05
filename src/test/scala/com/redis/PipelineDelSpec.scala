package com.redis

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class PipelineDelSpec extends Spec
with ShouldMatchers {

  val r = new RedisClient("localhost", 6379)

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
