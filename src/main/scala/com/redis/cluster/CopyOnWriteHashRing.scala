package com.redis.cluster

import java.util.concurrent.atomic.AtomicReference


class CopyOnWriteHashRing[T](nodes: Map[String, T], replicas: Int) {
  val ringRef = new AtomicReference[HashRing[T]](HashRing(nodes, replicas))

  def udpateNode(nodeName: String, node: T): T = {
    val ring = ringRef.get()
    val oldNode = ring.cluster(nodeName)
    val newRing = ring.moveNode(nodeName, node)
    if (!ringRef.compareAndSet(ring, newRing))
      udpateNode(nodeName, node)
    else
      oldNode
  }


  // get node for the key
  def getNode(key: Array[Byte]): T = ringRef.get().getNode(key)

  def cluster = ringRef.get().cluster
}
