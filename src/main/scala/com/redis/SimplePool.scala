package com.redis

import org.apache.commons.pool.PoolableObjectFactory
import java.util.concurrent.ConcurrentLinkedQueue


class SimplePool[T](factory: PoolableObjectFactory, initialIdle: Int, maxIdle: Int) {
  val queue = new ConcurrentLinkedQueue[T]()
  for (i <- 1 to initialIdle)
    queue.add(factory.makeObject().asInstanceOf[T])

  def borrowObject(): T = {
    Option(queue.poll()) match {
      case Some(v) => v
      case None => factory.makeObject().asInstanceOf[T]
    }
  }

  def returnObject(v: T) {
    if (queue.size() < maxIdle)
      queue.add(v)
    else
      factory.destroyObject(v)
  }

  def invalidateObject(v: T) {
    factory.destroyObject(v)
  }

  def close() {
    import collection.JavaConversions._
    queue.foreach(factory.destroyObject(_))
  }




}
