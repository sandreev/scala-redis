package com.redis.cluster

import com.redis.RedisClient
import com.twitter.util.CountDownLatch
import config.SimpleConfigManager
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClusterTransactionSpec extends Spec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  val r = new RedisCluster(new SimpleConfigManager(Map(
    "1" -> NodeConfig("localhost", 6379),
    "2" -> NodeConfig("localhost", 6379),
    "3" -> NodeConfig("localhost", 6379)
  ))) {
    val keyTag = Some(RegexKeyTag)

  }

  override def beforeEach() {
    r.flushdb
  }

  override def afterEach() {

  }

  override def afterAll() {
    r.close()
  }

  describe("transaction") {
    it("runs all commands in transaction") {
      r.transaction {
        r =>
          r.set("{1}:1", "2")
          r.set("{1}:2", "4")
          r.get("{1}:2")
      } should equal(Right(Some(List(true, true, Some("4")))))

      r.get("{1}:1") should equal(Some("2"))
      r.get("{1}:2") should equal(Some("4"))
    }

    it("rollbacks transaction in case of any exception") {
      val ex =new Exception
      r.transaction {
        r =>
          r.set("{1}:1", "2")
          r.set("{1}:2", "4")
          throw ex
      } should equal(Left(ex))

      r.get("{1}:1") should equal(None)
      r.get("{1}:2") should equal(None)
    }

    it("does not commit if watched key changed") {
      val r2 = new RedisClient("localhost", 6379)
      r2.select(2)
      val changeLatch = new CountDownLatch(1)
      val changedLatch = new CountDownLatch(1)


      val t1 = new Thread {
        override def run() {
          r.watch("{1}:1") should equal(true)
          r.get("{1}:1")
          changeLatch.countDown()
          changedLatch.await()
          r.transaction(_.set("{1}:1", "3")) should equal(Right(None))
        }
      }

      val t2 = new Thread {
        override def run() {
          changeLatch.await()
          r.set("{1}:1", "2")
          changedLatch.countDown()
        }
      }

      t1.start()
      t2.start()
      t1.join()
      t2.join()

      r.get("{1}:1") should equal(Some("2"))


    }

  }

  describe("stmLike") {
    it("reruns block if watch conflict detected") {

      val changeLatch = new CountDownLatch(1)
      val changedLatch = new CountDownLatch(1)

      r.set("{1}:1","1")


      val t1 = new Thread {
        override def run() {
          r.stmLike {
            r =>
              r.watch("{1}:1") should equal(true)
              val v = r.get("{1}:1").get
              changeLatch.countDown()
              changedLatch.await()
              (true, v)
          } {
            (r, v) =>
              r.set("{1}:1", v + v)
          }
        }
      }

      val t2 = new Thread {
        override def run() {
          changeLatch.await()
          r.set("{1}:1", "2")
          changedLatch.countDown()
        }
      }

      t1.start()
      t2.start()
      t1.join()
      t2.join()

      r.get("{1}:1") should equal(Some("22"))
    }

    it("breaks if precondition does not satisfy") {
      r.stmLike(r => (false, "111"))((r, v) => r.set(v,"123")) should equal(Right(None))
      r.get("111") should equal(None)
    }

  }

  describe("keys for different nodes") {
    it("prohibits different nodes within transaction") {
      val res = r.transaction {
        r =>
          for (i <- 1 to 100)
            r.set(i.toString, i.toString)
      }

      res.isLeft should equal(true)
      res.left.get.isInstanceOf[UnsupportedOperationException] should equal(true)
    }

    it("prohibits different nodes within stmlike precondition") {
      val res = r.stmLike {
        r =>
          for (i <- 1 to 100)
            r.set(i.toString, i.toString)
        (true, 1)
      } {
        (r, v) =>
      }

      res.isLeft should equal(true)
      res.left.get.isInstanceOf[UnsupportedOperationException] should equal(true)
    }

    it("prohibits different nodes in precondition and body") {
      val res = r.stmLike {
        r =>
          r.set("1","1")
          (true, 1)
      } {
        (r, _) =>
          r.set("1003","2")
      }

      res.isLeft should equal(true)
      res.left.get.isInstanceOf[UnsupportedOperationException] should equal(true)
    }


  }

}
