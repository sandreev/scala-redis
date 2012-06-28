package com.redis

import serialization.Format

object RedisClient {

  trait SortOrder

  case object ASC extends SortOrder

  case object DESC extends SortOrder

  trait Aggregate

  case object SUM extends Aggregate

  case object MIN extends Aggregate

  case object MAX extends Aggregate

}

trait Redis extends IO with Protocol {
  def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = {
    write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
    flush()
    result
  }

  def send[A](command: String)(result: => A): A = {
    write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
    flush()
    result
  }

  def cmd(args: Seq[Array[Byte]]) = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList
}

trait RedisCommand extends Redis
with Operations
with NodeOperations
with StringOperations
with ListOperations
with SetOperations
with SortedSetOperations
with HashOperations


trait Pipeline {
  def pipeline(f: RedisCommand => Any): Either[Exception, List[Either[Exception, Any]]]
}

class RedisClient(override val host: String, override val port: Int)
  extends RedisCommand
  with Pipeline
  with PubSub {

  connect

  def this() = this("localhost", 6379)

  override def toString = host + ":" + String.valueOf(port)

  def multi(f: MultiClient => Any): Option[List[Any]] = {
    send("MULTI")(asString) // flush reply stream
    try {
      val multiClient = new MultiClient(this)
      f(multiClient)
      send("EXEC")(asExec(multiClient.handlers))
    } catch {
      case e: RedisMultiExecException =>
        send("DISCARD")(asString)
        None
    }
  }

  def pipeline(f: RedisCommand => Any): Either[Exception, List[Either[Exception, Any]]] = {
    val pipe = pipelineBuffer

    val ex = try {
      f(pipe)
      None
    } catch {
      case e: Exception => Some(e)
    }

    ex match {
      case Some(e @ RedisConnectionException(_)) => Left(e)
      case _ => Right(pipe.flushAndGetResults())
    }
  }

  private[redis] def pipelineBuffer = new PipelineBuffer(this)

  class MultiClient(parent: RedisClient) extends RedisCommand {

    import serialization.Parse

    var handlers: Vector[() => Any] = Vector.empty


    override def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = {
      write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
      handlers :+= (() => result)
      flush()
      receive(singleLineReply).map(Parse.parseDefault)
      null.asInstanceOf[A] // ugh... gotta find a better way
    }

    override def send[A](command: String)(result: => A): A = {
      write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
      handlers :+= (() => result)
      flush()
      receive(singleLineReply).map(Parse.parseDefault)
      null.asInstanceOf[A]
    }

    val host = parent.host
    val port = parent.port

    // TODO: Find a better abstraction
    override def connected = parent.connected

    override def connect = parent.connect

    override def reconnect = parent.reconnect

    override def disconnect = parent.disconnect

    override def clearFd = parent.clearFd

    override def write(data: Array[Byte]) = parent.write(data)

    override def flush() {
      parent.flush()
    }

    override def readLine = parent.readLine

    override def readCounted(count: Int) = parent.readCounted(count)
  }

}

class PipelineBuffer(parent: RedisClient) extends RedisCommand {
  var handlers: Vector[() => Any] = Vector.empty

  private[redis] def flushAndGetResults(): List[Either[Exception, Any]] = {
    flush()
    val results = for (h <- handlers) yield {
      try {
        Right(h())
      } catch {
        case e: Exception =>
          Left(e)
      }
    }
    results.toList
  }

  override def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = {
    write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
    handlers :+= (() => result)
    null.asInstanceOf[A] // ugh... gotta find a better way
  }

  override def send[A](command: String)(result: => A): A = {
    write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
    handlers :+= (() => result)
    null.asInstanceOf[A]
  }

  val host = parent.host
  val port = parent.port

  // TODO: Find a better abstraction
  override def connected = parent.connected

  override def connect = parent.connect

  override def reconnect = parent.reconnect

  override def disconnect = parent.disconnect

  override def clearFd = parent.clearFd

  override def write(data: Array[Byte]) = parent.write(data)

  override def readLine = parent.readLine

  override def flush() {
    parent.flush()
  }

  override def readCounted(count: Int) = parent.readCounted(count)

}

