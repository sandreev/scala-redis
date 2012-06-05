package com.redis.cluster.config

import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.exception.ZkMarshallingError

object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}
