package com.redis

import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.pool.PoolableObjectFactory
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier}

class SimplePoolSpec extends WordSpec with MustMatchers {

  class PooledObject(i: Int) extends Function0[String] {
    val closed = new AtomicInteger()

    def apply() = {
      if (isClosed)
        throw new Exception("Resource closed")
      "OK" + i
    }

    def close() {
      closed.incrementAndGet()
    }

    def isClosed = closed.get() > 0
  }

  class TestObjectFactory extends PoolableObjectFactory {
    val i = new AtomicInteger()
    val createdObjects = new ConcurrentLinkedQueue[PooledObject]()

    def makeObject() = {
      Thread.sleep(16)
      val obj = new PooledObject(i.incrementAndGet())
      createdObjects.add(obj)
      obj
    }

    def destroyObject(obj: Any) {
      obj.asInstanceOf[PooledObject].close()
    }

    def validateObject(obj: Any) = true

    def activateObject(obj: Any) {}

    def passivateObject(obj: Any) {}
  }


  "pool" should {
    "contain specified number of inactive objects initially" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 2)

      factory.i.get() must equal(1)
      val obj = pool.borrowObject()
      obj() must equal("OK1")
      factory.i.get() must equal(1)
    }

    "create a new object if none in pool" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 2)

      factory.i.get() must equal(1)
      pool.borrowObject()
      val obj = pool.borrowObject()
      obj() must equal("OK2")
      factory.i.get() must equal(2)
    }

    "return object to pool if pool contains less inactives than specified" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 2)

      factory.i.get() must equal(1)
      val obj1 = pool.borrowObject()
      val obj2 = pool.borrowObject()
      factory.i.get() must equal(2)
      pool.returnObject(obj1)
      pool.returnObject(obj2)
      val obj3 = pool.borrowObject()
      val obj4 = pool.borrowObject()
      obj3() must equal("OK1")
      obj4() must equal("OK2")
      factory.i.get() must equal(2)
    }

    "destroy returned object if pool contains all necessary inactives" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 2)
      factory.i.get() must equal(1)
      var obj1 = pool.borrowObject()
      var obj2 = pool.borrowObject()
      var obj3 = pool.borrowObject()
      pool.returnObject(obj1)
      pool.returnObject(obj2)
      pool.returnObject(obj3)
      obj3.isClosed must equal(true)
      obj1 = pool.borrowObject()
      obj2 = pool.borrowObject()
      obj3 = pool.borrowObject()
      obj1() must equal("OK1")
      obj2() must equal("OK2")
      obj3() must equal("OK4")
    }

    "destroy invalidated object" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 2)
      factory.i.get() must equal(1)
      var obj1 = pool.borrowObject()
      pool.invalidateObject(obj1)
      obj1.isClosed must equal(true)
      obj1 = pool.borrowObject()
      obj1() must equal("OK2")
    }

    "work correctly in concurrent env" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 2)
      val nIterations = 1000
      val failProbability = 0.1
      val nThreads = 32
      val barrier = new CyclicBarrier(nThreads)

      class ConsumerThread extends Thread {
        override def run() {
          barrier.await()
          for (i <- 1 to nIterations) {
            val o = pool.borrowObject()
            if (math.random < failProbability)
              pool.invalidateObject(o)
            else
              pool.returnObject(o)
          }

        }
      }

      val threads = (1 to nThreads).map{
        i =>
          val t = new ConsumerThread
          t.start()
          t
      }
      threads.foreach(_.join)
    }
  }

  "pool closing" should {
    "be executed only once" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 8, 8)

      val nThreads = 32
      val barrier = new CyclicBarrier(nThreads)

      class CloseThread extends Thread {
        override def run() {
          barrier.await()
          pool.close()
        }
      }

      val threads = (1 to nThreads).map{
        i =>
          val t = new CloseThread
          t.start()
          t
      }
      threads.foreach(_.join)

      import scala.collection.JavaConversions._
      factory.createdObjects.forall(_.closed.get() == 1) must be(true)
    }

    "prohibit borrowing on closed pool" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 8, 8)
      pool.close()

      intercept[IllegalStateException]{
        pool.borrowObject()
      }
    }

    "destroy object if returned to closed pool" in {
      val factory = new TestObjectFactory
      val pool = new SimplePool[PooledObject](factory, 1, 8)
      val obj = pool.borrowObject()

      obj.isClosed must be(false)
      pool.close()
      obj.isClosed must be(false)
      pool.returnObject(obj)
      obj.isClosed must be(true)
    }
  }
}
