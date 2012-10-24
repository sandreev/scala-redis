package com.redis.cluster

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import com.redis.RedisBusinessException

@RunWith(classOf[JUnitRunner])
class ClusterPipelineListOperationsSpec extends Spec
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

  describe("lpush") {
    it("should add to the head of the list") {
      r.pipeline {
        p =>
          p.lpush("list-1", "foo")
          p.lpush("list-1", "bar")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2))))
    }
    it("should throw if the key has a non-list value") {
      val retList = r.pipeline {
        p =>
          p.set("anshin-1", "debasish")
          p.lpush("anshin-1", "bar")
      }.right.get
      retList(0) should equal(Right(true))
      retList(1) should equal(Left(new RedisBusinessException("ERR Operation against a key holding the wrong kind of value")))
    }
  }

  describe("lpush with variadic arguments") {
    it("should add to the head of the list") {
      r.pipeline {
        p =>
          p.lpush("list-1", "foo", "bar", "baz")
          p.lpush("list-1", "bag", "fog")
          p.lpush("list-1", "bag", "fog")
      }.right.get should equal(List(Right(Some(3)), Right(Some(5)), Right(Some(7))))
    }
  }
  //
  describe("rpush") {
    it("should add to the head of the list") {
      r.pipeline {
        p =>
          p.rpush("list-1", "foo")
          p.rpush("list-1", "bar")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2))))
    }
    it("should throw if the key has a non-list value") {
      val retList = r.pipeline {
        p =>
          p.set("anshin-1", "debasish")
          p.rpush("anshin-1", "bar")
      }.right.get
      retList(0) should equal(Right(true))
      retList(1) should equal(Left(new RedisBusinessException("ERR Operation against a key holding the wrong kind of value")))
    }
  }

  describe("rpush with variadic arguments") {
    it("should add to the head of the list") {
      r.pipeline {
        p =>
          p.rpush("list-1", "foo", "bar", "baz")
          p.rpush("list-1", "bag", "fog")
          p.rpush("list-1", "bag", "fog")
      }.right.get should equal(List(Right(Some(3)), Right(Some(5)), Right(Some(7))))
    }
  }

  describe("llen") {
    it("should return the length of the list") {
      r.pipeline {
        p =>
          p.lpush("list-1", "foo")
          p.lpush("list-1", "bar")
          p.llen("list-1")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some(2))))
    }
    it("should return 0 for a non-existent key") {
      r.pipeline {
        p => p.llen("list-2")
      }.right.get should equal(List(Right(Some(0))))
    }
    it("should throw for a non-list key") {
      val retList = r.pipeline {
        p =>
          p.set("anshin-1", "debasish")
          p.llen("anshin-1")
      }.right.get
      retList(0) should equal(Right(true))
      retList(1) should equal(Left(new RedisBusinessException("ERR Operation against a key holding the wrong kind of value")))
    }
  }

  describe("lrange") {
    it("should return the range") {
      val retList = r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.llen("list-1")
          p.lrange("list-1", 0, 4)
      }.right.get
      retList.slice(0, 6) should equal((1 to 6).toList.map(x => Right(Some(x))))
      retList(6).right.get should equal(Some(6))
      retList(7).right.get match {
        case x: Option[List[Option[_]]] => x.get should equal((1 to 5).toList.map(y => Some("" + y)))
      }
    }
    it("should return empty list if start > end") {
      val retList = r.pipeline {
        p =>
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.lrange("list-1", 2, 0)
      }.right.get
      retList.slice(0, 3) should equal((1 to 3).toList.map(x => Right(Some(x))))
      retList(3).right.get match {
        case x: Option[List[Option[_]]] => x.get should equal(List())
      }
    }
    it("should treat as end of list if end is over the actual end of list") {
      val retList = r.pipeline {
        p =>
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.lrange("list-1", 0, 7)
      }.right.get
      retList.slice(0, 3) should equal((1 to 3).toList.map(x => Right(Some(x))))
      retList(3).right.get match {
        case x: Option[List[Option[_]]] => x.get should equal(List(Some("1"), Some("2"), Some("3")))
      }
    }
  }
  //
  describe("ltrim") {
    it("should trim to the input size") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.ltrim("list-1", 0, 3)
          p.llen("list-1")
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(true) :: Right(Some(4)) :: Nil)
    }
    it("should should return empty list for start > end") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.ltrim("list-1", 6, 3)
          p.llen("list-1")
      }.right.get should equal((1 to 3).toList.map(x => Right(Some(x))) ::: Right(true) :: Right(Some(0)) :: Nil)
    }
    it("should treat as end of list if end is over the actual end of list") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.ltrim("list-1", 0, 12)
          p.llen("list-1")
      }.right.get should equal((1 to 3).toList.map(x => Right(Some(x))) ::: Right(true) :: Right(Some(3)) :: Nil)
    }
  }

  describe("lindex") {
    it("should return the value at index") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.lindex("list-1", 2)
          p.lindex("list-1", 3)
          p.lindex("list-1", -1)
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(Some("3")) :: Right(Some("4")) :: Right(Some("6")) :: Nil)
    }
    it("should return None if the key does not point to a list") {
      r.pipeline {
        p =>
          p.set("anshin-1", "debasish")
          p.lindex("list-1", 0)
      }.right.get should equal(List(Right(true), Right(None)))
    }
    it("should return empty string for an index out of range") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lindex("list-1", 8) // the protocol says it will return empty string
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some(3)), Right(None)))
    }
  }

  describe("lset") {
    it("should set value for key at index") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.lset("list-1", 2, "30")
          p.lindex("list-1", 2)
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(true) :: Right(Some("30")) :: Nil)

    }
    it("should generate error for out of range index") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lset("list-1", 12, "30")
      }.right.get should equal((1 to 3).toList.map(x => Right(Some(x))) ::: Left(RedisBusinessException("ERR index out of range")) :: Nil)
    }
  }

  describe("lrem") {
    it("should remove count elements matching value from beginning") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "4")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "hello")
          p.lrem("list-1", 2, "hello")
          p.llen("list-1")
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(Some(2)) :: Right(Some(4)) :: Nil)
    }
    it("should remove all elements matching value from beginning") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "4")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "hello")
          p.lrem("list-1", 0, "hello")
          p.llen("list-1")
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(Some(4)) :: Right(Some(2)) :: Nil)
    }
    it("should remove count elements matching value from end") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "4")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "hello")
          p.lpush("list-1", "hello")
          p.lrem("list-1", -2, "hello")
          p.llen("list-1")
          p.lindex("list-1", -2)
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(Some(2)) :: Right(Some(4)) :: Right(Some("4")) :: Nil)
    }
  }

  describe("lpop") {
    it("should pop the first one from head") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.lpop("list-1")
          p.lpop("list-1")
          p.lpop("list-1")
          p.llen("list-1")
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(Some("1")) :: Right(Some("2")) :: Right(Some("3")) :: Right(Some(3)) :: Nil)
    }
    it("should give nil for non-existent key") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpop("list-2")
          p.llen("list-1")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(None), Right(Some(2))))
    }
  }

  describe("rpop") {
    it("should pop the first one from tail") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.lpush("list-1", "4")
          p.lpush("list-1", "3")
          p.lpush("list-1", "2")
          p.lpush("list-1", "1")
          p.rpop("list-1")
          p.rpop("list-1")
          p.rpop("list-1")
          p.llen("list-1")
      }.right.get should equal((1 to 6).toList.map(x => Right(Some(x))) ::: Right(Some("6")) :: Right(Some("5")) :: Right(Some("4")) :: Right(Some(3)) :: Nil)
    }
    it("should give nil for non-existent key") {
      r.pipeline {
        p =>
          p.lpush("list-1", "6")
          p.lpush("list-1", "5")
          p.rpop("list-2")
          p.llen("list-1")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(None), Right(Some(2))))
    }
  }

  describe("rpoplpush") {
    it("should do") {
      r.pipeline {
        p =>
          p.rpush("list-1", "a")
          p.rpush("list-1", "b")
          p.rpush("list-1", "c")

          p.rpush("list-2", "foo")
          p.rpush("list-2", "bar")
          p.rpoplpush("list-1", "list-2")
          p.lindex("list-2", 0)
          p.llen("list-1")
          p.llen("list-2")
      }.right.get should equal((1 to 3).toList.map(x => Right(Some(x))) :::
        Right(Some(1)) :: Right(Some(2)) ::
        Right(Some("c")) :: Right(Some("c")) ::
        Right(Some(2)) :: Right(Some(3)) :: Nil)
    }

    it("should rotate the list when src and dest are the same") {
      r.pipeline {
        p =>
          p.rpush("list-1", "a")
          p.rpush("list-1", "b")
          p.rpush("list-1", "c")
          p.rpoplpush("list-1", "list-1")
          p.lindex("list-1", 0)
          p.lindex("list-1", 2)
          p.llen("list-1")
      }.right.get should equal((1 to 3).toList.map(x => Right(Some(x))) :::
        Right(Some("c")) :: Right(Some("c")) :: Right(Some("b")) ::
        Right(Some(3)) :: Nil)
    }

    it("should give None for non-existent key") {
      r.pipeline {
        p =>
          p.rpoplpush("list-1", "list-2")
          p.rpush("list-1", "a")
          p.rpush("list-1", "b")
          p.rpoplpush("list-1", "list-2")
      }.right.get should equal(List(Right(None), Right(Some(1)), Right(Some(2)), Right(Some("b"))))
    }
  }

  describe("lpush with newlines in strings") {
    it("should add to the head of the list") {
      r.pipeline {
        p =>
          p.lpush("list-1", "foo\nbar\nbaz")
          p.lpush("list-1", "bar\nfoo\nbaz")
          p.lpop("list-1")
          p.lpop("list-1")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some("bar\nfoo\nbaz")), Right(Some("foo\nbar\nbaz"))))
    }
  }

  describe("brpoplpush") {
    it("should throw UnsupportedOperationException") {
      r.pipeline {
        p =>
          p.rpush("list-1", "a")
          p.rpush("list-1", "b")
          p.rpush("list-1", "c")

          p.rpush("list-2", "foo")
          p.rpush("list-2", "bar")
          p.brpoplpush("list-1", "list-2", 2) //throw here and stop executing pipelines
          p.lindex("list-2", 0)
          p.llen("list-1")
          p.llen("list-2")
      }.right.get should equal(List(Right(Some(1)), Right(Some(2)), Right(Some(3)),
        Right(Some(1)), Right(Some(2))))

    }
  }

  describe("lpush with array bytes") {
    it("should add to the head of the list") {
      r.pipeline {
        p =>
          p.lpush("list-1", "foo\nbar\nbaz".getBytes("UTF-8"))
          p.lpop("list-1")
      }.right.get should equal(List(Right(Some(1)), Right(Some("foo\nbar\nbaz"))))
    }
  }

    describe("blpop") {
      it ("should throw UnsupportedOperationException") {
        r.pipeline {
          p =>
            r.blpop(3, "l1", "l2")
        }.right.get should equal(List())
      }
    }
}