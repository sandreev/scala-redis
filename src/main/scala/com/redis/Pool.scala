package com.redis

import org.apache.commons.pool._


private [redis] class RedisClientFactory(host: String, port: Int) extends PoolableObjectFactory {
  // when we make an object it's already connected
  def makeObject = new RedisClient(host, port) 

  // quit & disconnect
  def destroyObject(rc: Object): Unit = { 
    rc.asInstanceOf[RedisClient].quit // need to quit for closing the connection
    rc.asInstanceOf[RedisClient].disconnect // need to disconnect for releasing sockets
  }

  // noop: we want to have it connected
  def passivateObject(rc: Object): Unit = {} 
  def validateObject(rc: Object) = rc.asInstanceOf[RedisClient].connected == true

  // noop: it should be connected already
  def activateObject(rc: Object): Unit = {}
}

class RedisClientPool(host: String, port: Int) {
  val pool = new SimplePool[RedisClient](new RedisClientFactory(host, port), 1, 2)

  override def toString = host + ":" + String.valueOf(port)

  def withClient[T](body: RedisClient => T): T = {
    val client = pool.borrowObject
    var ioErrorOccurred = false
    try {
      body(client)
    } catch {
      case e: RedisConnectionException =>
        ioErrorOccurred = true
        throw e
    } finally {
      if (ioErrorOccurred)
        pool.invalidateObject(client)
      else
        pool.returnObject(client)
    }
  }



  // close pool & free resources
  def close = pool.close
}
