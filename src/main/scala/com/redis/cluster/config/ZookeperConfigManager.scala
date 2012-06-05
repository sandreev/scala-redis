package com.redis.cluster.config

import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import java.util.concurrent.{Callable, Executors, CopyOnWriteArrayList}
import org.slf4j.LoggerFactory
import java.util.StringTokenizer
import com.redis.cluster.{ConfigManager, NodeConfig, ClusterConfigListener}

class ZookeperConfigManager(cfg: ZkConfig) extends ConfigManager {

  import cfg._
  import collection.JavaConversions.asScalaIterator

  val logger = LoggerFactory.getLogger(getClass)

  val zkClient: ZkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, ZKStringSerializer)
  zkClient.subscribeDataChanges(zkNodesPath, ListenerComposite)

  val service = Executors.newSingleThreadExecutor()

  private def parseCofig(data: String): Map[String, NodeConfig] = {

    def parseEntry(entry: String): (String, NodeConfig) = {
      val nameHostPort = entry.split(":")
      try {
        (nameHostPort(0), NodeConfig(nameHostPort(1), nameHostPort(2).toInt))
      } catch {
        case e: IndexOutOfBoundsException =>
          throw new IllegalArgumentException("Invalid node config: " + entry)
      }
    }

    def parse(t: StringTokenizer, res: Map[String, NodeConfig]): Map[String, NodeConfig] = t.hasMoreTokens match {
      case true => parse(t, res + parseEntry(t.nextToken()))
      case false => res
    }

    parse(new StringTokenizer(data, "|"), Map.empty[String, NodeConfig])
  }


  object ListenerComposite extends IZkDataListener {
    val listeners = new CopyOnWriteArrayList[ClusterConfigListener]()

    def handleDataChange(dataPath: String, data: Any) {
      service.submit(new Runnable {
        def run() {
          val conf = parseCofig(data.toString)
          listeners.iterator.foreach {
            _.configUpdated(conf)
          }
        }
      })
    }

    def handleDataDeleted(dataPath: String) {
      logger.error("Attempt to delete cluster configuration!!!")
    }
  }

  def readConfig = service.submit(new Callable[Map[String, NodeConfig]] {
    def call() = parseCofig(zkClient.readData(zkNodesPath))
  }).get


  def addListener(listener: ClusterConfigListener) {
    service.submit(new Runnable {
      def run() {
        ListenerComposite.listeners.add(listener)
        val conf = parseCofig(zkClient.readData(zkNodesPath))
        listener.configUpdated(conf)
      }
    })
  }


}

