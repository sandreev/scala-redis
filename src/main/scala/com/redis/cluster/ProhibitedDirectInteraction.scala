package com.redis.cluster

import com.redis.serialization.Format

trait ProhibitedDirectInteraction {
  def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A =
    throw new UnsupportedOperationException("Operation not implemented for cluster mode")

  def send[A](command: String)(result: => A): A =
    throw new UnsupportedOperationException("Operation not implemented for cluster mode")


}
