package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import com.redis.RedisClient.DESC

@RunWith(classOf[JUnitRunner])
class ClusterPipelineSortedSetSpec extends Spec
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

  private def add = {
    r.pipeline {
      p =>
        p.zadd("hackers", 1965, "yukihiro matsumoto")
        p.zadd("hackers", 1953, "richard stallman", (1916, "claude shannon"), (1969, "linus torvalds"), (1940, "alan kay"), (1912, "alan turing"))
    }.right.get should equal(List(Right(Some(1)), Right(Some(5))))
  }

  describe("zadd") {
    it("should add based on proper sorted set semantics") {
      r.pipeline {
        p =>
          p.zadd("hackers", 1965, "yukihiro matsumoto")
          p.zadd("hackers", 1953, "richard stallman", (1916, "claude shannon"), (1969, "linus torvalds"), (1940, "alan kay"), (1912, "alan turing"))
          p.zadd("hackers", 1912, "alan turing")
          p.zcard("hackers")
      }.right.get should equal(List(Right(Some(1)), Right(Some(5)), Right(Some(0)), Right(Some(6))
      ))
    }
  }

  describe("zrem") {
    it("should remove") {
      add
      r.pipeline {
        p =>
          p.zrem("hackers", "alan turing")
          p.zrem("hackers", "alan kay", "linus torvalds")
          p.zrem("hackers", "alan kay", "linus torvalds")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some(0))))
    }
  }

  describe("zrange") {
    it("should get the proper range") {
      add
      r.pipeline {
        p =>
          p.zrange("hackers")
          p.zrangeWithScore("hackers")
      }.right.get.foreach {
        x => x.right.get match {
          case y:Option[List[_]] => y.get should have size (6)
        }
      }
    }
  }

  describe("zrank") {
    it("should give proper rank") {
      add
      r.pipeline {
        p =>
          p.zrank("hackers", "yukihiro matsumoto")
          p.zrank("hackers", "yukihiro matsumoto", reverse = true)
      }.right.get should equal(List(Right(Some(4)), Right(Some(1))))
    }
  }

  describe("zremrangebyrank") {
    it("should remove based on rank range") {
      add
      r.pipeline {
        p =>
          p.zremrangebyrank("hackers", 0, 2)
      }.right.get should equal(List(Right(Some(3))))
    }
  }

  describe("zremrangebyscore") {
    it("should remove based on score range") {
      add
      r.pipeline {
        p =>
          p.zremrangebyscore("hackers", 1912, 1940)
          p.zremrangebyscore("hackers", 0, 3)
      }.right.get should equal(List(Right(Some(3)), Right(Some(0))))
    }
  }
  describe("zunion") {
    it("should do a union") {
      val resList = r.pipeline {
        p =>
          p.zadd("hackers 1{tag}", 1965, "yukihiro matsumoto")
          p.zadd("hackers 1{tag}", 1953, "richard stallman")
          p.zadd("hackers 2{tag}", 1916, "claude shannon")
          p.zadd("hackers 2{tag}", 1969, "linus torvalds")
          p.zadd("hackers 3{tag}", 1940, "alan kay")
          p.zadd("hackers 4{tag}", 1912, "alan turing")
          // union with weight = 1
          p.zunionstore("hackers{tag}", List("hackers 1{tag}", "hackers 2{tag}", "hackers 3{tag}", "hackers 4{tag}"))
          p.zcard("hackers{tag}")

          p.zrangeWithScore("hackers{tag}")

          // union with modified weights
          p.zunionstoreWeighted("hackers weighted{tag}", Map("hackers 1{tag}" -> 1.0, "hackers 2{tag}" -> 2.0, "hackers 3{tag}" -> 3.0, "hackers 4{tag}" -> 4.0))
          p.zrangeWithScore("hackers weighted{tag}")
      }.right.get
      resList.slice(0, 6).foreach(x => x should equal(Right(Some(1))))
      resList(6) should equal(Right(Some(6)))
      resList(7) should equal(Right(Some(6)))
      resList(8).right.get match {
        case x: Option[List[Tuple2[_, _]]] => x.get.map(_._2) should equal(List(1912, 1916, 1940, 1953, 1965, 1969))
      }
      resList(9) should equal(Right(Some(6)))
      resList(10).right.get match {
        case x: Option[List[Tuple2[_, _]]] => x.get.map(_._2) should equal(List(1953, 1965, 3832, 3938, 5820, 7648))
      }
    }
  }

  describe("zinter") {
    it("should do an intersection") {
      val resList = r.pipeline {
        p =>
          p.zadd("hackers{tag}", 1912, "alan turing")
          p.zadd("hackers{tag}", 1916, "claude shannon")
          p.zadd("hackers{tag}", 1927, "john mccarthy")
          p.zadd("hackers{tag}", 1940, "alan kay")
          p.zadd("hackers{tag}", 1953, "richard stallman")
          p.zadd("hackers{tag}", 1954, "larry wall")
          p.zadd("hackers{tag}", 1956, "guido van rossum")
          p.zadd("hackers{tag}", 1965, "paul graham")
          p.zadd("hackers{tag}", 1965, "yukihiro matsumoto")
          p.zadd("hackers{tag}", 1969, "linus torvalds")

          p.zadd("baby boomers{tag}", 1948, "phillip bobbit")
          p.zadd("baby boomers{tag}", 1953, "richard stallman")
          p.zadd("baby boomers{tag}", 1954, "cass sunstein")
          p.zadd("baby boomers{tag}", 1954, "larry wall")
          p.zadd("baby boomers{tag}", 1956, "guido van rossum")
          p.zadd("baby boomers{tag}", 1961, "lawrence lessig")
          p.zadd("baby boomers{tag}", 1965, "paul graham")
          p.zadd("baby boomers{tag}", 1965, "yukihiro matsumoto")

          p.zinterstore("baby boomer hackers{tag}", List("hackers{tag}", "baby boomers{tag}"))
          p.zcard("baby boomer hackers{tag}")

          p.zrange("baby boomer hackers{tag}")

          // intersection with modified weights
          p.zinterstoreWeighted("baby boomer hackers weighted{tag}", Map("hackers{tag}" -> 0.5, "baby boomers{tag}" -> 0.5))
          p.zrangeWithScore("baby boomer hackers weighted{tag}")
      }.right.get
      resList.size should equal(23)
      (1 to 18).toList.foreach {
        i =>
          resList(i - 1) should equal(Right(Some(1)))
      }
      resList(18) should equal(Right(Some(5)))
      resList(19) should equal(Right(Some(5)))
      resList(20).right.get match {
        case x: Option[List[_]] => x.get should equal(List("richard stallman", "larry wall", "guido van rossum", "paul graham", "yukihiro matsumoto"))
      }
      resList(21) should equal(Right(Some(5)))
      resList(22).right.get match {
        case x: Option[List[Tuple2[_, _]]] => x.get.map(_._2) should equal(List(1953, 1954, 1956, 1965, 1965))
      }

    }
  }

  describe("zcount") {
    it("should return the number of elements between min and max") {
      add
      r.pipeline {
        p =>
          p.zcount("hackers", 1912, 1920)
      }.right.get should equal(List(Right(Some(2))))
    }
  }

  describe("zincrby") {
    it("should return the number of elements between min and max") {
      add
      r.pipeline {
        p =>
          p.zincrby("hackers", 10, "yukihiro matsumoto")
          p.zincrby("hackers", 10, "richard stallman")
          p.zincrby("hackers", 10, "claude shannon")
          p.zincrby("hackers", 10, "linus torvalds")
          p.zincrby("hackers", -1940, "alan kay")
          p.zincrby("hackers", 10, "alan turing")
      }.right.get should equal(List(Right(Some(1975)),Right(Some(1963)),Right(Some(1926)),Right(Some(1979)),Right(Some(0)),Right(Some(1922))))
    }
  }

  describe("zrangebyscore") {
    it("should return the elements between min and max") {
      add
      val resList = r.pipeline {
        p =>
          p.zrangebyscore("hackers", 1940, true, 1969, true, None)
          p.zrangebyscore("hackers", 1940, true, 1969, true, None, DESC)
      }.right.get
      resList(0).right.get match {
        case x: Option[List[_]] => x.get should equal(List("alan kay", "richard stallman", "yukihiro matsumoto", "linus torvalds"))
      }
      resList(1).right.get match {
        case x: Option[List[_]] => x.get should equal(List("linus torvalds", "yukihiro matsumoto", "richard stallman", "alan kay"))
      }
    }

    it("should return the elements between min and max and allow offset and limit") {
      add
      val resList = r.pipeline {
        p =>
          p.zrangebyscore("hackers", 1940, true, 1969, true, Some(0, 2))
          p.zrangebyscore("hackers", 1940, true, 1969, true, Some(0, 2), DESC)
          p.zrangebyscore("hackers", 1940, true, 1969, true, Some(3, 1))
          p.zrangebyscore("hackers", 1940, true, 1969, true, Some(3, 1), DESC)
          p.zrangebyscore("hackers", 1940, false, 1969, true, Some(0, 2))
          p.zrangebyscore("hackers", 1940, true, 1969, false, Some(0, 2), DESC)
      }.right.get
      resList(0).right.get match {
        case x: Option[List[_]] => x.get should equal(List("alan kay", "richard stallman"))
      }
      resList(1).right.get match {
        case x: Option[List[_]] => x.get should equal(List("linus torvalds", "yukihiro matsumoto"))
      }
      resList(2).right.get match {
        case x: Option[List[_]] => x.get should equal(List("linus torvalds"))
      }
      resList(3).right.get match {
        case x: Option[List[_]] => x.get should equal(List("alan kay"))
      }
      resList(4).right.get match {
        case x: Option[List[_]] => x.get should equal(List("richard stallman", "yukihiro matsumoto"))
      }
      resList(5).right.get match {
        case x: Option[List[_]] => x.get should equal(List("yukihiro matsumoto", "richard stallman"))
      }
    }
  }
  describe("redis cluster") {
    it("should save retrieve correct 100 sorted list with different keys ") {
      val NRELEM = 100
      r.pipeline {
        p =>
          (1 to NRELEM).foreach(i => {
            p.zadd("hackers" + i, 1900 + i, "Name" + i)
            p.zadd("hackers" + i, 1800 + i, "Other" + i)
            p.zadd("hackers" + i, 1700 + i, "New" + i)
          }
          )
      }.right.get should equal((1 to 3 * NRELEM).toList.map(x => Right(Some(1))))
      val resList = r.pipeline {
        p =>
          (1 to NRELEM).foreach(i => {
            p.zrange("hackers" + i)
          })
          (1 to NRELEM).foreach(i => {
            p.zrangeWithScore("hackers" + i)
          })
      }.right.get
      for (i <- 1 to NRELEM)
        resList(i - 1).right.get match {
          case x: Option[List[_]] => x.get should equal(List("New" + i, "Other" + i, "Name" + i))
        }
      for (i <- 1 to NRELEM)
        resList(NRELEM + i - 1).right.get match {
          case x: Option[List[_]] => x.get should equal(List(("New" + i, 1700 + i), ("Other" + i, 1800 + i), ("Name" + i, 1900 + i)))
        }


    }
  }
}