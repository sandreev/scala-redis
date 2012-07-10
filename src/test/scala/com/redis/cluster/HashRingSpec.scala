package com.redis.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import collection.immutable.TreeSet

class HashRingSpec extends WordSpec with MustMatchers {

  "constructor" should {
    "create correct ring" in {
      val nodes = Map("1" -> "node1", "2" -> "node2", "3" -> "node3")
      val ring = HashRing(nodes, 3)
      ring.cluster must be(nodes)
      ring.ring.size must be(nodes.size * 3)
      ring.ring.foldLeft(-1l){
        (prev, cur) =>
          (cur._1 > prev) must be(true)
          cur._1
      }
    }
  }

  "ringNode" should {
    "get a corresponding node if node's crc equals to key" in {
      val ring = Array((2l, 2), (4l,4), (6l,6))
      HashRing.ringNode(ring, 2) must be(2)
    }
    "get the first node if key is less than all ring keys" in {
      val ring = Array((2l, 2), (4l,4), (6l,6))
      HashRing.ringNode(ring, 1) must be(2)

    }
    "get the last node if key is greater than all ring keys" in {
      val ring = Array((2l, 2), (4l,4), (6l,6))
      HashRing.ringNode(ring, 7) must be(6)

    }
    "get the previous less node if the key is within ring" in {
      val ring = Array((2l, 2), (4l,4), (6l,6))
      HashRing.ringNode(ring, 3) must be(2)
    }

    "work as previously" in {
      val sortedKeys = TreeSet(2l,4l,6l)
      val ring = Map((2l, 2), (4l,4), (6l,6))
      val ring2 = Array((2l, 2), (4l,4), (6l,6))

      def oldFunc(crc: Long) = {
        if (sortedKeys contains crc) ring(crc)
        else {
          if (crc < sortedKeys.firstKey) ring(sortedKeys.firstKey)
          else if (crc > sortedKeys.lastKey) ring(sortedKeys.lastKey)
          else ring(sortedKeys.rangeImpl(None, Some(crc)).lastKey)
        }
      }

      HashRing.ringNode(ring2, 1l) must equal(oldFunc(1l))
      HashRing.ringNode(ring2, 7l) must equal(oldFunc(7l))
      HashRing.ringNode(ring2, 3l) must equal(oldFunc(3l))
    }
  }




}
