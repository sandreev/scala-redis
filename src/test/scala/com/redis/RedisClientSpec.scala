package com.redis

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{WordSpec, BeforeAndAfterAll, BeforeAndAfterEach, Spec}
import org.scalatest.matchers.ShouldMatchers
import com.twitter.util.CountDownLatch

@RunWith(classOf[JUnitRunner])
class RedisClientSpec extends WordSpec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)

  "client" should {
    "reconnect if ioexception occurred" in {
      val firstCommandFinished = new CountDownLatch(1)
      val connectionClosed = new CountDownLatch(1)

      val commandThread = new Thread() {
        override def run() {
          r.set("key1", "value1")
          firstCommandFinished.countDown()
          connectionClosed.await()
          r.set("key2", "value2")
        }
      }
      commandThread.start()
      firstCommandFinished.await()
      r.disconnect
      connectionClosed.countDown()
      commandThread.join()

      r.get("key1") should equal(Some("value1"))
      r.get("key2") should equal(Some("value2"))
    }

    "fail on non-connection exception" in {
      r.flushdb
      try {
        r.set("key1", "value1")
        r.rename("key1", "key1")
        r.set("key2", "value2")
      } catch  {
        case _ =>
      }

      r.get("key1") should equal(Some("value1"))
      r.get("key2") should equal(None)
    }
  }

}
