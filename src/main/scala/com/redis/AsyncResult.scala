package com.redis

sealed abstract class AsyncResult[+T] {
  def value: T
  def error: Option[Exception]
}


case class Success[T](val value: T) extends AsyncResult[T] {
  val error = None
}

case class ExecError(ex: Exception) extends AsyncResult[Nothing] {
  def value = throw ex
  val error = Some(ex)
}

