package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class MultiClientSpec extends Spec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)
  r.select(1)

  override protected def beforeAll() {
    r.flushdb
  }

  override def afterEach = {
    r.flushdb
  }

  override def afterAll = {
    r.disconnect
  }

  describe("multi1") {
    it("should do multid commands") {
      r.transaction {
        p =>
          p.set("key", "debasish")
          p.get("key")
          p.get("key1")
      } should equal(Right(Some(List(true, Some("debasish"), None))))
    }
  }

  describe("multi2") {
    it("should do multid commands") {
      r.transaction {
        p =>
          p.lpush("country_list", "france")
          p.lpush("country_list", "italy")
          p.lpush("country_list", "germany")
          p.incrby("country_count", 3)
          p.lrange("country_list", 0, -1)
      } should equal(Right(Some(List(Some(1), Some(2), Some(3), Some(3), Some(List(Some("germany"), Some("italy"), Some("france")))))))
    }
  }

  describe("multi3") {
    it("should handle errors properly in multid commands") {
      val thrown = r.transaction {
        p =>
          p.set("a", "abc")
          p.lpop("a")
      }.left.get

      thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
      r.get("a").get should equal("abc")
    }
  }


}
