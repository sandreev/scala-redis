package com.redis.cluster.config

import com.redis.cluster.{ClusterConfigListener, NodeConfig, ConfigManager}

class SimpleConfigManager(nodes: Map[String,NodeConfig]) extends ConfigManager {
  def readConfig = nodes

  def addListener(listener: ClusterConfigListener) {}
}
