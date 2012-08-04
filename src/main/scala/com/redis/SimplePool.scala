package com.redis

import org.apache.commons.pool.PoolableObjectFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference


class SimplePool[T](factory: PoolableObjectFactory, initialIdle: Int, maxIdle: Int) {

  private trait PoolState {
    def returnObject(v: T)
    def borrowObject: T
    def close()
  }

  private object Operating extends PoolState {
    val queue = new ConcurrentLinkedQueue[T]()

    for (i <- 1 to initialIdle)
      queue.add(factory.makeObject().asInstanceOf[T])

    def returnObject(v: T) {
      if (queue.size() < maxIdle)
        queue.add(v)
      else
        factory.destroyObject(v)
    }

    def borrowObject = Option(queue.poll()) match {
      case Some(v) => v
      case None => factory.makeObject().asInstanceOf[T]
    }

    def close() {
      import collection.JavaConversions._
      queue.foreach(factory.destroyObject(_))
    }
  }

  private object Closed extends PoolState {
    def returnObject(v: T) {
      factory.destroyObject(v)
    }

    def borrowObject = throw new IllegalStateException("Trying borrow from closed pool")

    def close() {}
  }

  private val stateRef = new AtomicReference[PoolState](Operating)

  def borrowObject(): T = stateRef.get().borrowObject

  def returnObject(v: T) {
    stateRef.get().returnObject(v)
  }

  def invalidateObject(v: T) {
    factory.destroyObject(v)
  }

  def close() {
    if (stateRef.compareAndSet(Operating, Closed))
      Operating.close()
  }




}
