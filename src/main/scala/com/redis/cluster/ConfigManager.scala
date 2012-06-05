package com.redis.cluster


trait ConfigManager {
  def readConfig: Map[String, NodeConfig]

  def addListener(listener: ClusterConfigListener)

}
