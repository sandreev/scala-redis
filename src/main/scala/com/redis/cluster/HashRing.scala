package com.redis.cluster

import java.util
import org.apache.hadoop.util.PureJavaCrc32New

class HashRing[T](val cluster: Map[String, T], val ring: Array[(Long, T)], replicas: Int) {

  import HashRing._

  def moveNode(nodeName: String, node: T) = {
    require(cluster.contains(nodeName), "Cluster does not contain node '" + nodeName + "'")
    HashRing(cluster + (nodeName -> node), replicas)
  }

  // get node for the key
  def getNode(key: Array[Byte]): T = {
    val crc = calculateChecksum(key)
    ringNode(ring, crc)
  }
}

object HashRing {
  def entryComparator[T] = new util.Comparator[(Long, T)] {
    def compare(o1: (Long, T), o2: (Long, T)) = o1._1.compareTo(o2._1)
  }

  def apply[T](nodes: Map[String, T], replicas: Int) = {
    val pairs =
      (for ((nodeName, node) <- nodes.toSeq;
           replica <- 1 to replicas) yield {
        (calculateChecksum((nodeName + ":" + replica).getBytes("UTF-8")), node)
      }).toArray
    util.Arrays.sort(pairs, 0, pairs.length, entryComparator[T])
    new HashRing[T](nodes, pairs, replicas)
  }

  // Computes the CRC-32 of the given String
  def calculateChecksum(value: Array[Byte]): Long = {
    val crc = new PureJavaCrc32New
    crc.update(value, 0, value.length)
    crc.getValue()
  }

  def ringNode[T](ring: Array[(Long, T)], crc: Long) = {
    util.Arrays.binarySearch(ring, (crc, null.asInstanceOf[T]), entryComparator[T]) match {
      case found if found >= 0 => ring(found)._2
      case greaterMax if greaterMax == - ring.length - 1 => ring(ring.length - 1)._2
      case candidate => ring(- candidate - 1)._2
    }

  }
}
