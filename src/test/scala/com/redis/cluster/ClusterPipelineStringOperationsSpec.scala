package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ClusterPipelineStringOperationsSpec extends Spec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with MockitoSugar {

  import org.mockito.Mockito._

  val conf = mock[ConfigManager]

  val hosts = Map("1" -> NodeConfig("localhost", 6379),
    "2" -> NodeConfig("localhost", 6380),
    "3" -> NodeConfig("localhost", 6381))

  when(conf.readConfig).thenReturn(
    hosts
  )

  val r = new RedisCluster(conf) {
    val keyTag = Some(RegexKeyTag)
  }

  override def beforeEach = {}

  override def afterEach = r.flushdb

  override def afterAll = r.close

  def successResults(list: Any*) = Right(list.map(Right(_)).toList)

  describe("set") {
    it("should set key/value pairs") {
      r.pipeline {
        r =>
          r.set("anshin-1", "debasish")
          r.set("anshin-2", "maulindu")
      } should be(successResults(true, true))
    }
  }

  describe("get") {
    it("should retrieve key/value pairs for existing keys") {
      r.pipeline {
        r =>
          r.set("anshin-1", "debasish")
          r.get("anshin-1")
      } should be(successResults(true, Some("debasish")))
    }
    it("should fail for non-existent keys") {
      r.pipeline {
        r =>
          r.get("anshin-2")
      } should be(successResults(None))
    }
  }

  describe("getset") {
    it("should set new values and return old values") {
      r.pipeline {
        r =>
          r.set("anshin-1", "debasish")
          r.get("anshin-1")
          r.getset("anshin-1", "maulindu")
          r.get("anshin-1")
      } should be(successResults(true, Some("debasish"), Some("debasish"), Some("maulindu")))
    }
  }

  describe("setnx") {
    it("should set only if the key does not exist") {
      r.pipeline {
        r =>
          r.set("anshin-1", "debasish")
          r.setnx("anshin-1", "maulindu")
          r.setnx("anshin-2", "maulindu")
      } should be(successResults(true, false, true))
    }
  }

  describe("setex") {
    it("should set values with expiry") {
      val key = "setex-1"
      val value = "value"
      r.pipeline {
        r =>
          r.setex(key, 1, value)
          r.get(key)
      } should be(successResults(true, Some(value)))
      Thread.sleep(2000)
      r.get(key) should be(None)
    }
  }

  describe("incr") {
    it("should increment by 1 for a key that contains a number") {
      r.pipeline {
        r =>
          r.set("anshin-1", "10")
          r.incr("anshin-1")
      } should be(successResults(true, Some(11)))
    }
    it("should reset to 0 and then increment by 1 for a key that contains a diff type") {
      val errMsg = r.pipeline {
        r =>
          r.set("anshin-2", "debasish")
          r.incr("anshin-2")
      }.right.get.apply(1).left.get.getMessage
      errMsg should startWith("ERR value is not an integer")
    }
    it("should increment by 5 for a key that contains a number") {
      r.pipeline {
        r =>
          r.set("anshin-3", "10")
          r.incrby("anshin-3", 5)
      } should be(successResults(true, Some(15)))
    }
    it("should reset to 0 and then increment by 5 for a key that contains a diff type") {
      r.pipeline {
        r =>
          r.set("anshin-4", "debasish")
          r.incrby("anshin-4", 5)
      }.right.get.apply(1).left.get.getMessage should startWith("ERR value is not an integer")
    }
  }

  describe("decr") {
    it("should decrement by 1 for a key that contains a number") {
      r.pipeline {
        r =>
          r.set("anshin-1", "10")
          r.decr("anshin-1")
      } should be(successResults(true, Some(9)))
    }
    it("should reset to 0 and then decrement by 1 for a key that contains a diff type") {
      r.pipeline {
        r =>
          r.set("anshin-2", "debasish")
          r.decr("anshin-2")
      }.right.get.apply(1).left.get.getMessage should startWith("ERR value is not an integer")
    }
    it("should decrement by 5 for a key that contains a number") {
      r.pipeline {
        r =>
          r.set("anshin-3", "10")
          r.decrby("anshin-3", 5)
      } should be(successResults(true, Some(5)))
    }
    it("should reset to 0 and then decrement by 5 for a key that contains a diff type") {
      r.pipeline {
        r =>
          r.set("anshin-4", "debasish")
          r.decrby("anshin-4", 5)
      }.right.get.apply(1).left.get.getMessage should startWith("ERR value is not an integer")
    }
  }

  describe("mget") {
    it("should get values for existing keys") {
      r.pipeline {
        r =>
          r.set("anshin-1", "debasish")
          r.set("anshin-2", "maulindu")
          r.set("anshin-3", "nilanjan")
          r.mget("anshin-1", "anshin-2", "anshin-3")
      } should be(successResults(true, true, true, Some(List(Some("debasish"), Some("maulindu"), Some("nilanjan")))))
    }
    it("should give None for non-existing keys") {
      r.pipeline {
        r =>
          r.set("anshin-1", "debasish")
          r.set("anshin-2", "maulindu")
          r.mget("anshin-1", "anshin-2", "anshin-4")
      } should be(successResults(true, true, Some(List(Some("debasish"), Some("maulindu"), None))))
    }
  }

  describe("mset") {
    it("should set all keys irrespective of whether they exist") {
      r.pipeline {
        r =>
          r.mset(
            ("anshin-1", "debasish"),
            ("anshin-2", "maulindu"),
            ("anshin-3", "nilanjan"))
      } should be(successResults(true))
    }

    it("should set all keys only if none of them exist") {
      r.pipeline {
        r =>
          r.msetnx(
            ("anshin-4", "debasish"),
            ("anshin-5", "maulindu"),
            ("anshin-6", "nilanjan"))
          r.msetnx(
            ("anshin-7", "debasish"),
            ("anshin-8", "maulindu"),
            ("anshin-6", "nilanjan"))
          r.msetnx(
            ("anshin-4", "debasish"),
            ("anshin-5", "maulindu"),
            ("anshin-6", "nilanjan"))
      } should be(successResults(true, false, false))
    }
  }

  describe("get with spaces in keys") {
    it("should retrieve key/value pairs for existing keys") {
      r.pipeline {
        r =>
          r.set("anshin software", "debasish ghosh")
          r.get("anshin software")
          r.set("test key with spaces", "I am a value with spaces")
          r.get("test key with spaces")
      } should be(successResults(
        true,
        Some("debasish ghosh"),
        true,
        Some("I am a value with spaces")
      ))
    }
  }

  describe("get with newline values") {
    it("should retrieve key/value pairs for existing keys") {
      r.pipeline {
        r =>
          r.set("anshin-x", "debasish\nghosh\nfather")
          r.get("anshin-x")
      } should be(successResults(
        true,
        Some("debasish\nghosh\nfather")
      ))
    }
  }

  describe("setrange") {
    it("should set value starting from offset") {
      r.pipeline {
        r =>
          r.set("key1", "hello world")
          r.setrange("key1", 6, "redis")
          r.get("key1")
          r.setrange("key2", 6, "redis")
          r.get("key2")
      } should be(successResults(
        true,
        Some(11),
        Some("hello redis"),
        Some(11),
        Some(new String(Array.fill[Byte](6)(0.toByte)) + "redis")
      ))
    }
  }

  describe("getrange") {
    it("should get value starting from start") {
      r.pipeline {
        r =>
          r.set("mykey", "This is a string")
          r.getrange[String]("mykey", 0, 3)
          r.getrange[String]("mykey", -3, -1)
          r.getrange[String]("mykey", 0, -1)
          r.getrange[String]("mykey", 10, 100)
      } should be(successResults(
        true,
        Some("This"),
        Some("ing"),
        Some("This is a string"),
        Some("string")
      ))
    }
  }

  describe("strlen") {
    it("should return the length of the value") {
      r.pipeline {
        r =>
          r.set("mykey", "Hello World")
          r.strlen("mykey")
          r.strlen("nonexisting")
      } should be(successResults(
        true,
        Some(11),
        Some(0)
      ))
    }
  }

  describe("append") {
    it("should append value to that of a key") {
      r.pipeline {
        r =>
          r.exists("mykey")
          r.append("mykey", "Hello")
          r.append("mykey", " World")
          r.get[String]("mykey")
      } should be(successResults(
        false,
        Some(5),
        Some(11),
        Some("Hello World")
      ))
    }
  }

  describe("setbit") {
    it("should set of clear the bit at offset in the string value stored at the key") {
      r.setbit("mykey", 7, 1) should equal(Some(0))
      r.setbit("mykey", 7, 0) should equal(Some(1))
      String.format("%x", new java.math.BigInteger(r.get("mykey").get.getBytes("UTF-8"))) should equal("0")
    }
  }

  describe("getbit") {
    it("should return the bit value at offset in the string") {
      r.setbit("mykey", 7, 1) should equal(Some(0))
      r.getbit("mykey", 0) should equal(Some(0))
      r.getbit("mykey", 7) should equal(Some(1))
      r.getbit("mykey", 100) should equal(Some(0))
    }
  }


}
