package com.redis.cluster

import com.redis.Log


trait LookupLogger {
  self: Log =>

  val loggedKeyMasks = Option(System.getProperty("loggedkeys")) match {
    case Some(csv) => csv.split(",").map(_.trim).toSeq
    case _ => Seq.empty[String]
  }

  def logLookup(key: Any, tag: Option[Array[Byte]], node: Any) {
    val keyString = key.toString
    if (loggedKeyMasks.exists(keyString.indexOf(_) >= 0)) tag match {
      case Some(arr) => info("lookup: " + keyString + " => tag " + new String(arr) + ", node " + node)
      case _ => info("lookup: " + keyString + " => node " + node)
    }

  }

}
