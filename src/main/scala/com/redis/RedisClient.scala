package com.redis

import serialization.Format
import java.io.IOException

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
  def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A

  def send[A](command: String)(result: => A): A

  def cmd(args: Seq[Array[Byte]]) = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList
}

trait SyncCommand {
  self: IO with Log =>

  private def syncCommand[A](command: Array[Byte], attemptsRemaining: Int)(result: () => A)(implicit format: Format): A = {
    try {
      write(command)
      flush()
      result()
    } catch {
      case e if (e.isInstanceOf[RedisConnectionException] || e.isInstanceOf[IOException]) && attemptsRemaining > 1 =>
        warn("Got IO error while performing operation - reconnecting", e)
        reconnect
        syncCommand(command, attemptsRemaining - 1)(result)
    }
  }

  def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = {
    syncCommand(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))), 2)(() => result)
  }

  def send[A](command: String)(result: => A): A = {
    syncCommand(Commands.multiBulk(List(command.getBytes("UTF-8"))), 2)(() => result)
  }

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
  def pipeline(f: RedisCommand with Pipeline => Any): Either[Exception, List[Either[Exception, Any]]]
}

class RedisClient(override val host: String, override val port: Int)
  extends RedisCommand
  with SyncCommand
  with Pipeline
  with PubSub {

  connect

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

  def pipeline(f: RedisCommand with Pipeline => Any): Either[Exception, List[Either[Exception, Any]]] = {
    val pipe = pipelineBuffer

    val ex = try {
      f(pipe)
      None
    } catch {
      case e: Exception => Some(e)
    }


    ex match {
      case Some(e : RedisConnectionException) => Left(e)
      case other =>
        try {
          pipe.flush()
          Right(pipe.readResults() ::: (other.toList.map(Left(_))))
        } catch {
          case e: Exception => Left(e)
        }
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

    override def connect = throw new UnsupportedOperationException("cannot initiate connection within transaction")

    override def disconnect = parent.disconnect

    override def write(data: Array[Byte]) = parent.write(data)

    override def flush() {
      parent.flush()
    }

    override def readLine = parent.readLine

    override def readCounted(count: Int) = parent.readCounted(count)
  }

}

class PipelineBuffer(parent: RedisClient) extends RedisCommand with Pipeline {
  var handlers: List[() => Any] = List.empty
  var handlersCount = 0


  private[redis] def readResults(): List[Either[Exception, Any]] = {
    val results = for (h <- handlers.reverse) yield {
      try {
        Right(h())
      } catch {
        case e: Exception =>
          Left(e)
      }
    }
    results
  }

  override def send[A](command: String, args: Seq[Any])(result: => A)(implicit format: Format): A = {

    write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
    handlers ::= (() => result    )
    handlersCount += 1
    if (handlersCount % 256 == 0)
      flush()
    null.asInstanceOf[A] // ugh... gotta find a better way
  }

  override def send[A](command: String)(result: => A): A = {
    write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
    handlers ::= (() => result)
    handlersCount += 1
    if (handlersCount % 256 == 0)
      flush()
    null.asInstanceOf[A]
  }


  def pipeline(f: RedisCommand with Pipeline => Any) = {
    f(this)
    Left(new IllegalStateException("Results of nested pipeline"))
  }

  val host = parent.host
  val port = parent.port

  // TODO: Find a better abstraction
  override def connected = parent.connected

  override def connect = throw new UnsupportedOperationException("Cannot initiate connection within pipeline")

  override def disconnect = parent.disconnect

  override def write(data: Array[Byte]) = parent.write(data)

  override def readLine = parent.readLine

  override def flush() {
    parent.flush()
  }

  override def readCounted(count: Int) = parent.readCounted(count)

}

