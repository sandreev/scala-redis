package com.redis.cluster.config

import org.scalatest.WordSpec
import org.I0Itec.zkclient.ZkClient
import java.util.Properties
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.ConcurrentLinkedQueue
import com.redis.cluster.{ClusterConfigListener, NodeConfig}

class ZookeeperConfigManagerSpec extends WordSpec with ShouldMatchers {

  val props = new Properties()
  props.setProperty("zk.connect","localhost:2181")

  val zkConfig = new ZkConfig(props)
  val zkClient = new ZkClient(zkConfig.zkConnect,zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs, ZKStringSerializer)
  val configManager = new ZookeperConfigManager(zkConfig)

  def setConfig(cfg: Map[String, NodeConfig]) {
    zkClient.createPersistent(zkConfig.zkNodesPath, true)
    zkClient.writeData(zkConfig.zkNodesPath, cfg.toSeq.map{ case (name, hostPort) => name + ":" + hostPort.host + ":" + hostPort.port}
      .mkString("|")
    )
  }

  "readConfig" should {
    "read data correctly" in {
      val conf = Map("1" -> NodeConfig("1", 1),
        "2" -> NodeConfig("1", 2),
        "3" -> NodeConfig("2", 1) )

      setConfig(conf)

      configManager.readConfig should be(conf)
    }
  }

  "listeners" should {
    "get data update" in {
      val conf = Map("1" -> NodeConfig("1", 1),
        "2" -> NodeConfig("1", 2),
        "3" -> NodeConfig("2", 1) )

      setConfig(conf)

      val newConf = Map("1" -> NodeConfig("1", 1),
        "2" -> NodeConfig("3", 1),
        "3" -> NodeConfig("2", 1) )

      val queue = new ConcurrentLinkedQueue[Map[String, NodeConfig]]()

      configManager.addListener(new ClusterConfigListener {
        def configUpdated(newConfig: Map[String, NodeConfig]) {
          queue.add(newConfig)
        }
      })
      setConfig(newConf)
      Thread.sleep(1000)

      var lastConf: Any = null
      while(!queue.isEmpty)
        lastConf = queue.remove()

      lastConf should be(newConf)

    }
  }



}
