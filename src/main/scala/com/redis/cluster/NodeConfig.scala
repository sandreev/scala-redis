package com.redis.cluster

case class NodeConfig(host: String, port: Int) {
  override def toString = host + ":" + port
}
