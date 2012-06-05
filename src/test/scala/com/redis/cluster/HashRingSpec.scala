package com.redis.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class HashRingSpec extends WordSpec with MustMatchers {

  "constructor" should {
    "create correct ring" in {
      val nodes = Map("1" -> "node1", "2" -> "node2", "3" -> "node3")
      val ring = HashRing(nodes, 3)
      ring.cluster must be(nodes)
      ring.ring.size must be(nodes.size * 3)
    }
  }

  "addNode" should {
    "prohibit duplicated name" in {
      val nodes = Map("1" -> "node1", "2" -> "node2", "3" -> "node3")
      val ring = HashRing(nodes, 3)

      intercept[IllegalArgumentException] {
        ring.addNode("1", "node11")
      }
    }
  }

}
