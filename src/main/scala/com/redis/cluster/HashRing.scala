package com.redis.cluster

import java.util.zip.CRC32
import collection.immutable.SortedMap

class HashRing[T](val cluster: Map[String, T], val ring: SortedMap[Long, T], replicas: Int) {

  import HashRing._

  // adds a node to the hash ring (including a number of replicas)
  def addNode(nodeName: String, node: T) = {
    require(!cluster.contains(nodeName), "Cluster already contains node '" + nodeName + "'")
    val pairs = (1 to replicas).map {
      replica => (calculateChecksum((nodeName + ":" + replica).getBytes("UTF-8")), node)
    }
    new HashRing[T](cluster + (nodeName -> node), ring ++ pairs, replicas)
  }

  // remove node from the ring
  def removeNode(nodeName: String) = {
    require(cluster.contains(nodeName), "Cluster does not contain node '" + nodeName + "'")
    val keys = (1 to replicas).map {
      replica => calculateChecksum((nodeName + ":" + replica).getBytes("UTF-8"))
    }
    new HashRing[T](cluster - nodeName, ring -- keys, replicas)
  }

  // get node for the key
  def getNode(key: Seq[Byte]): T = {
    val crc = calculateChecksum(key)
    if (ring contains crc) ring(crc)
    else {
      if (crc < ring.firstKey) ring.head._2
      else if (crc > ring.lastKey) ring.last._2
      else ring.rangeImpl(None, Some(crc)).last._2
    }
  }
}

object HashRing {
  def apply[T](nodes: Map[String, T], replicas: Int) = {
    val pairs =
      for ((nodeName, node) <- nodes.toSeq;
           replica <- 1 to replicas) yield {
        (calculateChecksum((nodeName + ":" + replica).getBytes("UTF-8")), node)
      }
    new HashRing[T](nodes, SortedMap(pairs: _*), replicas)
  }

  // Computes the CRC-32 of the given String
  def calculateChecksum(value: Seq[Byte]): Long = {
    val checksum = new CRC32
    checksum.update(value.toArray)
    checksum.getValue
  }
}
