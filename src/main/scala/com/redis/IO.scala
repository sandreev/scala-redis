package com.redis

import java.io._
import java.net.Socket

import serialization.Parse.parseStringSafe

trait IO extends Log {
  val host: String
  val port: Int

  var socket: Socket = _
  var out: OutputStream = _
  var in: InputStream = _
  var db: Int = _

  def connected = {
    socket != null
  }

  // Connects the socket, and sets the input and output streams.
  def connect: Boolean = {
    socket = new Socket(host, port)
    socket.setSoTimeout(0)
    socket.setKeepAlive(true)
    socket.setTcpNoDelay(true)
    out = new BufferedOutputStream(socket.getOutputStream)
    in = new BufferedInputStream(socket.getInputStream)
    true
  }

  def reconnect = disconnect & connect

  // Disconnects the socket.
  def disconnect: Boolean = {
    try {
      if (socket != null)
        socket.close
      if (out != null)
        out.close
      if (in != null)
        in.close
      true
    } catch {
      case x =>
        error("Error closing socket", x)
        false
    }
  }

  // Wrapper for the socket write operation.
  def withOutput[T](op: OutputStream => T) = try {
    op(out)
  } catch {
    case e: IOException => throw new RedisConnectionException("Error writing to socket", e)
  }


  def withInput[T](op: InputStream => T) = try {
    op(in)
  } catch {
    case e: IOException => throw new RedisConnectionException("Error reading from socket", e)
  }


  // Writes data to a socket using the specified block.
  def write(data: Array[Byte]) = {
    ifDebug("C: " + parseStringSafe(data))
    withOutput(_.write(data))
  }

  def flush() {
    ifDebug("C: flush")
    withOutput(_.flush)
  }


  private val crlf = List(13, 10)

  def readLine: Array[Byte] = withInput {
    in =>
      var delimiter = crlf
      var found: List[Int] = Nil
      var build = new scala.collection.mutable.ArrayBuilder.ofByte

      while (delimiter != Nil) {
        val next = in.read
        if (next < 0) return null
        if (next == delimiter.head) {
          found ::= delimiter.head
          delimiter = delimiter.tail
        } else {
          if (found != Nil) {
            delimiter = crlf
            build ++= found.reverseMap(_.toByte)
            found = Nil
          }
          build += next.toByte
        }
      }
      build.result
  }

  def readCounted(count: Int): Array[Byte] = withInput {
    in =>
      val arr = new Array[Byte](count)
      var cur = 0
      while (cur < count) {
        val bytesRead = in.read(arr, cur, count - cur)
        if (bytesRead == 0)
          return null
        cur += bytesRead
      }
      arr
  }
}
