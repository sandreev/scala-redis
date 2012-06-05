package com.redis.cluster

import config.ZookeeperConfigManager
import io.Source
import org.I0Itec.zkclient.ZkClient


object RedisClusterZkUpdater {
  def main(args: Array[String]) {
    if (args.size != 3) {
      println("USAGE: mapping_file zookeeper_connect zookeeper_path")
      System.exit(1)
    }

    val redisNodesPath = args(2)
    val nodesMapping = Source.fromFile(args(0)).getLines()

    println("READ %s nodes from file %s".format(nodesMapping.size, args(0)))

    val zkClient = new ZkClient(args(1))

    val nodeData = nodesMapping.map(_.trim).mkString(ZookeeperConfigManager.NODES_SEPARATOR)

    if (!zkClient.exists(redisNodesPath)) {
      zkClient.createPersistent(redisNodesPath, nodeData)
      println("Created redis nodes record in zookeeper and loaded it with data")
    } else {
      zkClient.writeData(redisNodesPath, nodeData)
      println("Loaded redis nodes mapping in zookeeper")
    }
  }
}
