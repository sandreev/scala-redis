package com.redis.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class ClusterConfigDiffSpec extends WordSpec with ShouldMatchers {

  "ClusterConfigDiff" should {
    "find new nodes" in {
      val oldConf = Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 1))
      val newConf = oldConf + ("3" -> NodeConfig("3", 1))
      ClusterConfigDiff(oldConf, newConf).newNodes should be(newConf -- oldConf.keys)
    }

    "find removed nodes" in {
      val oldConf = Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 1))
      val newConf = oldConf - "2"
      ClusterConfigDiff(oldConf, newConf).deletedNodes should be(oldConf -- newConf.keys)
    }

    "find updated nodes" in {
      val oldConf = Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 1))
      val newConf = Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 2))
      ClusterConfigDiff(oldConf, newConf).updatedNodes should be(Map("2" -> NodeConfig("2", 2)))
    }

    "detect no changes" in {
      val oldConf = Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 1))
      val newConf = Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 1))

      val diff = ClusterConfigDiff(oldConf, newConf)
      diff.deletedNodes.isEmpty should be(true)
      diff.updatedNodes.isEmpty should be(true)
      diff.newNodes.isEmpty should be(true)
    }

    "detect multiple changes" in {
      val oldConf = Map(
        "1" -> NodeConfig("1", 1),
        "2" -> NodeConfig("2", 1),
        "3" -> NodeConfig("3", 1),
        "4" -> NodeConfig("4", 1),
        "5" -> NodeConfig("5", 1),
        "6" -> NodeConfig("6", 1))

      val newConf = Map(
        "7" -> NodeConfig("7", 1),
        "8" -> NodeConfig("8", 1),
        "3" -> NodeConfig("3", 2),
        "4" -> NodeConfig("4", 2),
        "5" -> NodeConfig("5", 1),
        "6" -> NodeConfig("6", 1))

      val diff = ClusterConfigDiff(oldConf, newConf)
      diff.deletedNodes should be(Map("1" -> NodeConfig("1", 1), "2" -> NodeConfig("2", 1)))
      diff.updatedNodes should be(Map("3" -> NodeConfig("3", 2), "4" -> NodeConfig("4", 2)))
      diff.newNodes should be(Map("7" -> NodeConfig("7", 1), "8" -> NodeConfig("8", 1)))
    }



  }

}
