package com.redis.cluster


import com.redis.serialization.Format
import com.redis._
import scala.Some

private[cluster] class SingleNodeConstraint(parent: RedisCluster) extends ClusterRedisCommand with NodeManager with Transactional {
  val host = parent.host
  val port = parent.port
  private val hr: HashRing[RedisClientPool] = parent.hr.ringRef.get()

  private class PoolAndClient(val pool: RedisClientPool, val client: RedisClient) {
    def this(pool: RedisClientPool) = this(pool, pool.pool.borrowObject())
  }

  private var poolAndClient: Option[PoolAndClient] = None

  private def poolAndClient(keys: Any*)(implicit format: Format): PoolAndClient = poolAndClient match {
    case None =>
      poolAndClient = Some(new PoolAndClient(parent.poolForKeys(hr, keys: _*)))
      poolAndClient.get
    case Some(pc) if (parent.poolForKeys(hr, keys: _*) eq pc.pool) =>
      pc
    case Some(_) =>
      throw new UnsupportedOperationException("Multinode transactions not supported")
  }

  private def multiClient(keys: Any*)(implicit format: Format): RedisCommand with Transactional =
    poolAndClient(keys: _*).client

  def inSameNode[T](keys: Any*)(body: (RedisCommand) => T)(implicit format: Format) =
    body(multiClient(keys: _*))

  def withNode[T](key: Any)(body: (RedisCommand) => T)(implicit format: Format) =
    body(multiClient(key))

  def onAllConns[T,R](body: (RedisClient) => T)(merge: Iterable[T] => R) = throw new UnsupportedOperationException("Multinode transactions not supported")

  def groupByNodes[T,R](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(merge: Iterable[(Seq[Any], T)] => R)(implicit format: Format) = {
    val allKeys = key :: keys.toList
    val client = multiClient(allKeys: _*)
    Some(merge((allKeys, body(client, allKeys)) :: Nil))
  }

  def transaction(f: RedisCommand => Any): Either[Exception, Option[List[Any]]] = (new Transaction).transaction(f)

  class Transaction extends ClusterRedisCommand with NodeManager with RawTransactional with DefaultTransactional {
    val host = SingleNodeConstraint.this.host
    val port = SingleNodeConstraint.this.port


    def openTx() {}

    def commit() = driver.map(_.commit()).getOrElse(Some(Nil))

    def rollback() {
      driver.map(_.rollback())
    }

    private var driver: Option[RedisCommand with RawTransactional] = None

    def driver(keys: Any*)(implicit format: Format): RedisCommand = {
      val poolAndClient = SingleNodeConstraint.this.poolAndClient(keys: _*)
      driver match {
        case Some(r) => r
        case None =>
          val tx = poolAndClient.client.transactioned
          tx.openTx()
          driver = Some(tx)
          tx
      }
    }

    def inSameNode[T](keys: Any*)(body: (RedisCommand) => T)(implicit format: Format) =
      body(driver(keys: _*))

    def withNode[T](key: Any)(body: (RedisCommand) => T)(implicit format: Format) =
      body(driver(key))

    def onAllConns[T, R](body: (RedisClient) => T)(merge: Iterable[T] => R) = SingleNodeConstraint.this.onAllConns(body)(merge)

    def groupByNodes[T,R](key: Any, keys: Any*)(body: (RedisCommand, Seq[Any]) => T)(merge: Iterable[(Seq[Any], T)] => R)(implicit format: Format) = {
      val allKeys = key :: keys.toList
      val client = driver(allKeys: _*)
      Some(merge((allKeys, body(client, allKeys)) :: Nil))
    }
  }

  def close(err: Option[Exception]) {
    (poolAndClient, err) match {
      case (Some(p), Some(e)) => p.pool.pool.invalidateObject(p.client)
      case (Some(p), None) => p.pool.pool.returnObject(p.client)
      case _ =>
    }


  }

}