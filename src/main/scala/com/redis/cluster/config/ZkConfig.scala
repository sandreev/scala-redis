package com.redis.cluster.config

import java.util.Properties

class ZkConfig(props: Properties) {
  import ZkConfig._

  val zkConnect = getString(props, "zk.connect")
  val zkSessionTimeoutMs = getInt(props, "zk.sessiontimeout.ms", 6000)
  val zkConnectionTimeoutMs = getInt(props, "zk.connectiontimeout.ms", zkSessionTimeoutMs)
  val zkNodesPath = getString(props, "zk.rediscluster.nodes", "/rediscluster/nodes")
}

object ZkConfig {
  private def get[T](props: Properties, name: String, default: T)(f: String => T) = Option(props.getProperty(name)) match {
    case Some(v) => f(v)
    case None => default
  }

  private def get[T](props: Properties, name: String)(f: String => T) =  Option(props.getProperty(name)) match {
    case Some(v) => f(v)
    case None => throw new IllegalArgumentException("Missing mandatory property " + name)
  }

  def getString(props: Properties, name: String) = get(props, name){v => v}
  def getString(props: Properties, name: String, default: String) = get(props, name, default){v => v}
  def getInt(props: Properties, name: String, default: Int) = get(props,name,default){_.toInt}
}
