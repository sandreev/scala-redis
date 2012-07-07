package com.redis

import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import java.io.IOException


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

  val poolConfig = new GenericObjectPool.Config()
  poolConfig.minIdle = 1
  poolConfig.maxIdle = 3
  poolConfig.minEvictableIdleTimeMillis = 30000
  poolConfig.timeBetweenEvictionRunsMillis = 30000

  val pool = new GenericObjectPool(new RedisClientFactory(host, port), poolConfig)


  override def toString = host + ":" + String.valueOf(port)

  def withClient[T](body: RedisClient => T): T = {
    val client = pool.borrowObject.asInstanceOf[RedisClient]
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
