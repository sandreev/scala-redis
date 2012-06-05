package com.redis.cluster

import config.{ZookeeperConfigManager, ZookeperConfigManager, ZKStringSerializer, ZkConfig}
import java.util.Properties
import org.I0Itec.zkclient.ZkClient
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, WordSpec}

class RedisClusterUpdateSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {

  val props = new Properties()
  props.setProperty("zk.connect","localhost:2181")

  val zkConfig = new ZkConfig(props)
  val zkClient = new ZkClient(zkConfig.zkConnect,zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs, ZKStringSerializer)
  val configManager = new ZookeperConfigManager(zkConfig)

  def setConfig(cfg: Map[String, NodeConfig]) {
    zkClient.createPersistent(zkConfig.zkNodesPath, true)
    zkClient.writeData(zkConfig.zkNodesPath, cfg.toSeq.map{ case (name, hostPort) => name + ":" + hostPort.host + ":" + hostPort.port}
      .mkString(ZookeeperConfigManager.NODES_SEPARATOR)
    )
  }

  override protected def beforeAll() {
    setConfig(Map("1" -> NodeConfig("192.168.56.101", 6379),
      "2" -> NodeConfig("192.168.56.101", 6380),
      "3" -> NodeConfig("192.168.56.101", 6381)))
  }

  "cluster" should {
    "not fail on node update" in {
      val cluster = new RedisCluster(configManager) {
        val keyTag = Some(RegexKeyTag)
      }

      setConfig(Map("1" -> NodeConfig("192.168.56.101", 6379),
        "2" -> NodeConfig("192.168.56.101", 6380),
        "3" -> NodeConfig("192.168.56.101", 6382)))

      Thread.sleep(3000)

      var clients = Set.empty[String]
      cluster.onAllConns( clients += _.toString )

      clients should be(Set("192.168.56.101:6379", "192.168.56.101:6380", "192.168.56.101:6382"))
    }
  }


}
