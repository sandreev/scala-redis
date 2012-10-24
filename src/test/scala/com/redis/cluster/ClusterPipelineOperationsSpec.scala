package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar


@RunWith(classOf[JUnitRunner])
class ClusterPipelineOperationsSpec extends Spec
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


  describe("keys") {
    it("should not work in pipeline mode") {
      r.pipeline(_.keys("*")).right.get.head.isLeft should be(true)
    }
  }

  describe("randkey") {
    it("should not work in pipeline mode") {
      r.pipeline(_.randkey).right.get.head.isLeft should be(true)

    }
  }

  describe("rename") {
    it("should prohibit renaming key with moving to another node") {
      r.set("{key}:1", "v1")
      r.pipeline {
        r =>
          r.rename("{key}:1", "{key1234}")
      }.right.get.head.isLeft should be(true)

    }

    it("should rename key if staying in the same node") {
      r.set("{key}:2", "v1")
      r.pipeline {
        r =>
          r.rename("{key}:2", "{key}:3")
      } should be(successResults(
        true
      ))
    }
  }

  describe("renamenx") {
    it("should prohibit renaming key with moving to another node") {
      r.set("{key}:3", "v1")
      r.pipeline {
        r =>
          r.renamenx("{key}:3", "{key1234}")
      }.right.get.head.isLeft should be(true)
    }

    it("should rename key if staying in the same node and the target key does not exist") {
      r.set("{key}:3", "v1")
      r.pipeline {
        r =>
          r.renamenx("{key}:3", "{key}:4")
          r.get("{key}:4")
      } should be(successResults(
        true,
        Some("v1")
      ))

    }

    it("should not rename key if staying in the same node and the target key exists") {
      r.set("{key}:3", "v1")
      r.set("{key}:4", "v2")
      r.pipeline {
        r =>
          r.renamenx("{key}:3", "{key}:4")
          r.get("{key}:4")
      } should be(successResults(
        false,
        Some("v2")
      ))

    }
  }

  describe("dbsize") {
    it("should not work in pipeline mode") {
      r.pipeline(_.dbsize).right.get.head.isLeft should be(true)
    }
  }

  describe("exists") {
    it("should return true if the key exists") {
      r.set("{key}:5", "v1")
      r.pipeline {
        r =>
          r.exists("{key}:5")
      } should be(successResults(true))
    }

    it("should return false otherwise") {
      r.pipeline {
        r =>
          r.exists("{key}:6")
      } should be(successResults(false))

    }
  }

  describe("delete") {
    it("should remove all specified keys") {
      r.set("key6", "1")
      r.set("key7", "1")
      r.set("key8", "1")
      r.set("key9", "1")
      r.set("key10", "1")
      r.pipeline {
        r =>
          r.del("key6", "key7", "key8", "key9", "key10", "key11")
      } should be(successResults(Some(5)))
    }

  }

  describe("type") {
    it("should return a type of the key") {
      r.set("key11", "1")
      r.pipeline{
        r =>
          r.getType("key11")
      } should be(successResults(Some("string")))

    }

    it("should return nothing if the key does not exist") {
      r.pipeline{
        r =>
          r.getType("key12")
      } should be(successResults(Some("none")))
    }

  }

  /*describe("expire") {
    it("should set expire period") {

    }

  }

  describe("expireat") {
    it("should set expire date") {

    }

  }

  describe("select") {
    it("should not work in pipeline mode") {

    }
  }

  describe("flush") {
    it("should not work in pipeline mode") {

    }
  }

  describe("flushall") {
    it("should not work in pipeline mode") {

    }
  }

  describe("move") {
    it("should not work in pipeline mode") {

    }
  }

  describe("quit") {
    it("should not work in pipeline mode") {

    }
  }

  describe("auth") {
    it("should not work in pipeline mode") {

    }
  }

  describe("watch") {
    it("should put a watch to the key") {

    }
  } */

}
