package com.redis.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import collection.immutable.TreeSet
import java.util.zip.CRC32
import org.apache.hadoop.util.PureJavaCrc32New
import java.util.{StringTokenizer, Properties}

class HashRingSpec extends WordSpec with MustMatchers {

  "constructor" should {
    "create correct ring" in {
      val nodes = Map("1" -> "node1", "2" -> "node2", "3" -> "node3")
      val ring = HashRing(nodes, 3)
      ring.cluster must be(nodes)
      ring.ring.size must be(nodes.size * 3)
      ring.ring.foldLeft(-1l) {
        (prev, cur) =>
          (cur._1 > prev) must be(true)
          cur._1
      }
    }
  }

  "ringNode" should {
    "get a corresponding node if node's crc equals to key" in {
      val ring = Array((2l, 2), (4l, 4), (6l, 6))
      HashRing.ringNode(ring, 2) must be(2)
    }
    "get the first node if key is less than all ring keys" in {
      val ring = Array((2l, 2), (4l, 4), (6l, 6))
      HashRing.ringNode(ring, 1) must be(2)

    }
    "get the last node if key is greater than all ring keys" in {
      val ring = Array((2l, 2), (4l, 4), (6l, 6))
      HashRing.ringNode(ring, 7) must be(6)

    }
    "get the previous less node if the key is within ring" in {
      val ring = Array((2l, 2), (4l, 4), (6l, 6))
      HashRing.ringNode(ring, 3) must be(2)
    }

    "calculate hash as previously" in {
      for (i <- 1 to 1000) {
        val key = "key" + i
        val oldCrc = {
          val crc = new CRC32()
          crc.update(key.getBytes("UTF8"))
          crc.getValue
        }
        val newCrc = HashRing.calculateChecksum(key.getBytes("UTF8"))



        newCrc must equal(oldCrc)

      }

    }

    "work as previously" in {
      val sortedKeys = TreeSet(2l, 4l, 6l)
      val ring = Map((2l, 2), (4l, 4), (6l, 6))
      val ring2 = Array((2l, 2), (4l, 4), (6l, 6))

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

    "keep keys distribution" in {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/node-keys.properties"))

      import collection.JavaConversions._
      val expectedNodeKeys = props.entrySet().map{
        e =>
          val node = e.getKey.toString
          val tokenizer = new StringTokenizer(e.getValue.toString, ",")
          var keys = List.empty[String]
          while(tokenizer.hasMoreTokens)
            keys ::= tokenizer.nextToken().trim
          node -> keys.toSet
      }.toMap


      val ring = HashRing(expectedNodeKeys.keySet.map(v => (v, v)).toMap, 160)
      val actualNodeKeys = expectedNodeKeys.values.flatten.map {
        key =>
          (ring.getNode(key.getBytes("UTF-8")), key)
      }.groupBy(_._1)
      .map(e => (e._1, e._2.map(_._2)toSet))
      .toMap

      actualNodeKeys must equal(expectedNodeKeys)
    }
  }


}
