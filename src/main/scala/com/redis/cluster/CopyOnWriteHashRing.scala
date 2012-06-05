package com.redis.cluster

import java.util.concurrent.atomic.AtomicReference


class CopyOnWriteHashRing[T](nodes: Map[String, T], replicas: Int) {
  val ringRef = new AtomicReference[HashRing[T]](HashRing(nodes, replicas))

  def addNode(nodeName: String, node: T) {
    val ring = ringRef.get()
    if (!ringRef.compareAndSet(ring, ring.addNode(nodeName, node)))
      addNode(nodeName, node)
  }

  def udpateNode(nodeName: String, node: T): T = {
    val ring = ringRef.get()
    val oldNode = ring.cluster(nodeName)
    val newRing = ring.removeNode(nodeName).addNode(nodeName, node)
    if (!ringRef.compareAndSet(ring, newRing))
      udpateNode(nodeName, node)
    else
      oldNode
  }

  // remove node from the ring
  def removeNode(nodeName: String): T = {
    val ring = ringRef.get()
    val newRing = ring.removeNode(nodeName)
    if (!ringRef.compareAndSet(ring, newRing))
      removeNode(nodeName)
    else
      ring.cluster(nodeName)
  }

  // get node for the key
  def getNode(key: Seq[Byte]): T = ringRef.get().getNode(key)

  def cluster = ringRef.get().cluster
}
