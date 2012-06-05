package com.redis.cluster


trait ClusterConfigListener {
  def configUpdated(newConfig: Map[String, NodeConfig])

}
