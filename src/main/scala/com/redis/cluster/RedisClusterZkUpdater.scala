package com.redis.cluster

import config.{ZKStringSerializer, ZookeeperConfigManager}
import io.Source
import org.I0Itec.zkclient.ZkClient

object RedisClusterZkUpdater {
  def main(args: Array[String]) {
    val (mappingFile, zkConnect, redisNodesPath) = args.toList match {
      case Nil =>
        println("USAGE: mapping_file zookeeper_connect zookeeper_path")
        throw new RuntimeException("Illegal number of arguments")
      case m :: Nil => (m, "localhost:2181", "/rediscluster/nodes")
      case m :: z :: Nil => (m, z, "/rediscluster/nodes")
      case m :: z :: r :: _ => (m, z, r)
    }

    val nodesMapping = Source.fromFile(mappingFile).getLines().toList.map(_.trim)
    println("READ %s nodes from file %s".format(nodesMapping.size, mappingFile))

    val zkClient = new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)

    val nodeData = nodesMapping.mkString(ZookeeperConfigManager.NODES_SEPARATOR)

    if (!zkClient.exists(redisNodesPath)) {
      zkClient.createPersistent("/rediscluster")
      zkClient.createPersistent(redisNodesPath, nodeData)
      println("Created redis nodes record in zookeeper and loaded it with data")
    } else {
      zkClient.writeData(redisNodesPath, nodeData)
      println("Loaded redis nodes mapping in zookeeper")
    }

    println("Current data in zookeeper: " + zkClient.readData(redisNodesPath))

    zkClient.close()
  }
}
