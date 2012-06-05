package com.redis.cluster

case class ClusterConfigDiff(newNodes: Map[String, NodeConfig],
                             updatedNodes: Map[String, NodeConfig],
                             deletedNodes: Map[String, NodeConfig])

object ClusterConfigDiff {
  def apply(oldCluster: Map[String, NodeConfig], newCluster: Map[String, NodeConfig]): ClusterConfigDiff = {
    val newNodes = newCluster -- oldCluster.keys
    val removedNodes = oldCluster -- newCluster.keys
    val changedNodes = newCluster.filter {
      case (name, cfg) if oldCluster.contains(name) => cfg != oldCluster(name)
      case _ => false
    }
    ClusterConfigDiff(newNodes, changedNodes, removedNodes)
  }
}
