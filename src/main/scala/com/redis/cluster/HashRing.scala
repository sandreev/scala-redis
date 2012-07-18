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

  def getLookupInfo(key: Array[Byte]) = {
    val crc = calculateChecksum(key)
    val (pos, actualPos) = ringLookupInfo(ring, crc)
    LookupInfo(crc, pos, actualPos)
  }
}

object HashRing {
  case class LookupInfo(crc: Long, ringPosition: Int, actualNodeIdx: Int) {
    override def toString = "[crc=" + crc + ", positionCandidate=" + (- ringPosition - 1) + ", actualRingPosition=" + actualNodeIdx + "]"
  }

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
      case lessMin if lessMin == -1 => ring(0)._2
      case candidate => ring(- candidate - 2)._2
    }

  }


  def ringLookupInfo[T](ring: Array[(Long, T)], crc: Long) = {
    val pos = util.Arrays.binarySearch(ring, (crc, null.asInstanceOf[T]), entryComparator[T]);
    val actualPos =   pos match {
      case found if found >= 0 => found
      case greaterMax if greaterMax == - ring.length - 1 => ring.length - 1
      case lessMin if lessMin == -1 => 0
      case candidate => - candidate - 2
    }
    (pos, actualPos)

  }
}
