package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import com.redis.{RedisClient, RedisConnectionException}

@RunWith(classOf[JUnitRunner])
class ClusterPipelineSpec extends Spec
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
      val ex = new Exception("want to discard")
      val res = r.pipeline {
        p =>
          p.set("a", "abc")
          throw ex
      }

      res.right.get should equal(List(Right(true), Left(ex)))
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
    it("should keep order of commands with keys mapping to different nodes") {
      val res = r.pipeline {
        p =>
          for (i <- 1 to 1000)
            p.rpush(i % 100, i / 100)
      }

      val expectedRes = Right((
        for (i <- 1 to 10;
             j <- 1 to 100) yield Right(Some(i))
        ).toList)

      res should equal(expectedRes)
    }
  }

  describe("pipeline6") {
    it("should respect keytags") {
      val res = r.pipeline(
        p =>
          for (i <- 1 to 1000)
            p.set("key{tag}" + i, i)

      )

      def allOrNone(cfg: NodeConfig): Boolean = {
        val client = new RedisClient(cfg.host, cfg.port)
        client.keys() match {
          case Some(Nil) =>
            println("No keys found in " + cfg)
            true
          case Some(listOption) =>
            println("" + listOption.size + " keys found in " + cfg)
            listOption.size == 1000 && listOption.forall(_.isDefined)
          case _ => false
        }
      }

      allOrNone(hosts("1")) should equal(true)
      allOrNone(hosts("2")) should equal(true)
      allOrNone(hosts("3")) should equal(true)
    }
  }
}