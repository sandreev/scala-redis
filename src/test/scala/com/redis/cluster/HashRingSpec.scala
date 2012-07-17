package com.redis.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import collection.immutable.TreeSet

import com.redis._

import serialization._

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

    "create production ring" in {
      val nodes = Map(
        "redis_6379" -> 6379,
        "redis_6380" -> 6380,
        "redis_6381" -> 6381,
        "redis_6382" -> 6382,
        "redis_6383" -> 6383,
        "redis_6384" -> 6384,
        "redis_6385" -> 6385,
        "redis_6386" -> 6386,
        "redis_6387" -> 6387,
        "redis_6388" -> 6388,
        "redis_6389" -> 6389,
        "redis_6390" -> 6390,
        "redis_6391" -> 6391,
        "redis_6392" -> 6392,
        "redis_6393" -> 6393,
        "redis_6394" -> 6394,
        "redis_6395" -> 6395,
        "redis_6396" -> 6396,
        "redis_6397" -> 6397,
        "redis_6398" -> 6398,
        "redis_6399" -> 6399,
        "redis_6400" -> 6400,
        "redis_6401" -> 6401,
        "redis_6402" -> 6402,
        "redis_6403" -> 6403,
        "redis_6404" -> 6404,
        "redis_6405" -> 6405,
        "redis_6406" -> 6406,
        "redis_6407" -> 6407,
        "redis_6408" -> 6408,
        "redis_6409" -> 6409,
        "redis_6410" -> 6410,
        "redis_6411" -> 6411,
        "redis_6412" -> 6412,
        "redis_6413" -> 6413,
        "redis_6414" -> 6414,
        "redis_6415" -> 6415,
        "redis_6416" -> 6416,
        "redis_6417" -> 6417,
        "redis_6418" -> 6418,
        "redis_6419" -> 6419,
        "redis_6420" -> 6420,
        "redis_6421" -> 6421,
        "redis_6422" -> 6422,
        "redis_6423" -> 6423,
        "redis_6424" -> 6424,
        "redis_6425" -> 6425,
        "redis_6426" -> 6426,
        "redis_6427" -> 6427,
        "redis_6428" -> 6428,
        "redis_6429" -> 6429,
        "redis_6430" -> 6430,
        "redis_6431" -> 6431,
        "redis_6432" -> 6432,
        "redis_6433" -> 6433,
        "redis_6434" -> 6434,
        "redis_6435" -> 6435,
        "redis_6436" -> 6436,
        "redis_6437" -> 6437,
        "redis_6438" -> 6438,
        "redis_6439" -> 6439,
        "redis_6440" -> 6440,
        "redis_6441" -> 6441,
        "redis_6442" -> 6442,
        "redis_6443" -> 6443,
        "redis_6444" -> 6444,
        "redis_6445" -> 6445,
        "redis_6446" -> 6446,
        "redis_6447" -> 6447,
        "redis_6448" -> 6448,
        "redis_6449" -> 6449,
        "redis_6450" -> 6450,
        "redis_6451" -> 6451,
        "redis_6452" -> 6452,
        "redis_6453" -> 6453,
        "redis_6454" -> 6454,
        "redis_6455" -> 6455,
        "redis_6456" -> 6456,
        "redis_6457" -> 6457,
        "redis_6458" -> 6458,
        "redis_6459" -> 6459,
        "redis_6460" -> 6460,
        "redis_6461" -> 6461,
        "redis_6462" -> 6462,
        "redis_6463" -> 6463,
        "redis_6464" -> 6464,
        "redis_6465" -> 6465,
        "redis_6466" -> 6466,
        "redis_6467" -> 6467,
        "redis_6468" -> 6468,
        "redis_6469" -> 6469,
        "redis_6470" -> 6470,
        "redis_6471" -> 6471,
        "redis_6472" -> 6472,
        "redis_6473" -> 6473,
        "redis_6474" -> 6474,
        "redis_6475" -> 6475,
        "redis_6476" -> 6476,
        "redis_6477" -> 6477,
        "redis_6478" -> 6478,
        "redis_6479" -> 6479,
        "redis_6480" -> 6480,
        "redis_6481" -> 6481,
        "redis_6482" -> 6482,
        "redis_6483" -> 6483,
        "redis_6484" -> 6484,
        "redis_6485" -> 6485,
        "redis_6486" -> 6486,
        "redis_6487" -> 6487,
        "redis_6488" -> 6488,
        "redis_6489" -> 6489,
        "redis_6490" -> 6490,
        "redis_6491" -> 6491,
        "redis_6492" -> 6492,
        "redis_6493" -> 6493,
        "redis_6494" -> 6494,
        "redis_6495" -> 6495,
        "redis_6496" -> 6496,
        "redis_6497" -> 6497,
        "redis_6498" -> 6498,
        "redis_6499" -> 6499,
        "redis_6500" -> 6500,
        "redis_6501" -> 6501,
        "redis_6502" -> 6502,
        "redis_6503" -> 6503,
        "redis_6504" -> 6504,
        "redis_6505" -> 6505,
        "redis_6506" -> 6506
      )

      val ring = HashRing(nodes, 160)

      val format = implicitly[Format]

      ring.getNode(RegexKeyTag.tag(format("v:{33747184:1}:4:2012-07-16")).get) must equal(6395)
      ring.getNode(RegexKeyTag.tag(format("v:{33747184:1}")).get) must equal(6395)
      ring.getNode(RegexKeyTag.tag(format("o:{33747184:1}")).get) must equal(6395)
      ring.getNode(RegexKeyTag.tag(format("v:{33747184:1}:11:2012-07-17")).get) must equal(6395)

      RegexKeyTag.tag(format("v:{33747184:1}:4:2012-07-16")).get must equal(RegexKeyTag.tag(format("v:{33747184:1}:11:2012-07-17")).get)
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
