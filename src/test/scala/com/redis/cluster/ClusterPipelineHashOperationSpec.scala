package com.redis.cluster

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ClusterPipelineHashOperationSpec extends Spec
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

  describe("hset") {
    it("should set and get fields") {
      r.pipeline {
        r =>
          r.hset("hash1", "field1", "val")
          r.hget("hash1", "field1")
      } should be(Right(List(Right(true), Right(Some("val")))))
    }

    it("should set and get maps") {
      r.pipeline {
        r =>
          r.hmset("hash2", Map("field1" -> "val1", "field2" -> "val2"))
          r.hmget("hash2", "field1")
          r.hmget("hash2", "field1", "field2")
          r.hmget("hash2", "field1", "field2", "field3")
      } should be(successResults(
        true,
        Some(Map("field1" -> "val1")),
        Some(Map("field1" -> "val1", "field2" -> "val2")),
        Some(Map("field1" -> "val1", "field2" -> "val2"))))
    }

    it("should increment map values") {
      r.pipeline {
        r =>
          r.hincrby("hash3", "field1", 1)
          r.hget("hash3", "field1") should be(Some("1"))
      } should be(Right(List(Right(Some(1)), Right(Some("1")))))
    }

    it("should check existence") {
      r.pipeline {
        r =>
          r.hset("hash4", "field1", "val")
          r.hexists("hash4", "field1")
          r.hexists("hash4", "field2")
      } should be(successResults(
        true,
        true,
        false
      ))
    }

    it("should delete fields") {
      r.pipeline {
        r =>
          r.hset("hash5", "field1", "val")
          r.hexists("hash5", "field1")
          r.hdel("hash5", "field1")
          r.hexists("hash5", "field1")
          r.hmset("hash5", Map("field1" -> "val1", "field2" -> "val2"))
          r.hdel("hash5", "field1", "field2")
      } should be(successResults(
        true,
        true,
        Some(1),
        false,
        true,
        Some(2)
      ))
    }

    it("should return the length of the fields") {
      r.pipeline {
        r =>
          r.hmset("hash6", Map("field1" -> "val1", "field2" -> "val2"))
          r.hlen("hash6") should be(Some(2))
      } should be(successResults(
        true,
        Some(2)
      ))
    }

    it("should return the aggregates") {
      r.pipeline {
        r =>
          r.hmset("hash7", Map("field1" -> "val1", "field2" -> "val2"))
          r.hkeys("hash7")
          r.hvals("hash7")
          r.hgetall("hash7")
      } should be(successResults(
        true,
        Some(List("field1", "field2")),
        Some(List("val1", "val2")),
        Some(Map("field1" -> "val1", "field2" -> "val2"))
      ))
    }
  }

  describe("hsetnx") {
    it("should set a value if the key/field does not exist") {
      r.pipeline{
        r =>
          r.hsetnx("hash8", "field1", "val1")
          r.hget("hash8", "field1")
          r.hsetnx("hash8", "field2", "val2")
          r.hget("hash8", "field2")
      } should be(successResults(true, Some("val1"), true, Some("val2")))
    }
    it("should not set a value if the key and field exist") {
      r.pipeline{
        r =>
          r.hsetnx("hash9", "field1", "val1")
          r.hsetnx("hash9", "field1", "val2")
          r.hget("hash9", "field1")
      } should be(successResults(true, false, Some("val1")))
    }
  }

  describe("hgetall") {
    it("should return all fields and values if exist") {
      r.pipeline{
        r =>
          r.hmset("hash10", List("f1" -> "v1", "f2" -> "v2"))
          r.hmget("hash10", "f1", "f2", "f3")
      } should be(successResults(
        true,
        Some(Map("f1" -> "v1", "f2" -> "v2"))
      ))
    }

    it("should return empty map if the key does not exist") {
      r.pipeline{
        r =>
          r.hmget("hash11", "f1", "f2", "f3")
      } should be(successResults(
        Some(Map.empty)
      ))

    }
  }
}
