package com.redis

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import com.twitter.util.CountDownLatch

@RunWith(classOf[JUnitRunner])
class TransactionSpec extends Spec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)
  r.select(2)

  override def beforeEach() {
    r.flushdb
  }

  override def afterEach() {

  }

  override def afterAll() {
    r.disconnect
  }

  describe("transaction") {
    it("runs all commands in transaction") {
      r.transaction {
        r =>
          r.set("1", "2")
          r.set("3", "4")
          r.get("3")
      } should equal(Right(Some(List(true, true, Some("4")))))

      r.get("1") should equal(Some("2"))
      r.get("3") should equal(Some("4"))
    }

    it("rollbacks transaction in case of any exception") {
      val ex =new Exception
      r.transaction {
        r =>
          r.set("1", "2")
          r.set("3", "4")
          throw ex
      } should equal(Left(ex))

      r.get("1") should equal(None)
      r.get("3") should equal(None)
    }

    it("does not commit if watched key changed") {
      val r2 = new RedisClient("localhost", 6379)
      r2.select(2)
      val changeLatch = new CountDownLatch(1)
      val changedLatch = new CountDownLatch(1)


      val t1 = new Thread {
        override def run() {
          r.watch("1") should equal(true)
          r.get("1")
          changeLatch.countDown()
          changedLatch.await()
          r.transaction(_.set("1", "3")) should equal(Right(None))
        }
      }

      val t2 = new Thread {
        override def run() {
          changeLatch.await()
          r2.set("1", "2")
          changedLatch.countDown()
        }
      }

      t1.start()
      t2.start()
      t1.join()
      t2.join()

      r.get("1") should equal(Some("2"))
      r2.disconnect

    }

  }

  describe("stmLike") {
    it("reruns block if watch conflict detected") {
      val r2 = new RedisClient("localhost", 6379)
      r2.select(2)
      val changeLatch = new CountDownLatch(1)
      val changedLatch = new CountDownLatch(1)

      r.set("1","1")


      val t1 = new Thread {
        override def run() {
          r.stmLike {
            r =>
              r.watch("1") should equal(true)
              val v = r.get("1").get
              changeLatch.countDown()
              changedLatch.await()
              (true, v)
          } {
            (r, v) =>
              r.set("1", v + v)
          }
        }
      }

      val t2 = new Thread {
        override def run() {
          changeLatch.await()
          r2.set("1", "2")
          changedLatch.countDown()
        }
      }

      t1.start()
      t2.start()
      t1.join()
      t2.join()

      r.get("1") should equal(Some("22"))
      r2.disconnect
    }

    it("breaks if precondition does not satisfy") {
      r.stmLike(r => (false, "111"))((r, v) => r.set(v,"123")) should equal(Right(None))
      r.get("111") should equal(None)
    }

    it("gets precondition exception to account") {
      val ex = new Exception
      r.stmLike(_ => throw ex)((r, _) => r.set(1,1)) should equal(Left(ex))
    }

  }


}
