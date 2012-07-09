package com.redis.stress

import com.redis.RedisCommand


trait PipelineStress {
  def name: String
  def warmup()
  def resetEnv()

  def withRedis(f: RedisCommand => Any)



  def opsInIteration: Int = 10000
  def nIterations: Int = 30
  def nChecks: Int = 2


  def onlyIncrs(r: RedisCommand, nOps: Int)  {
    for (i <- 1 to nOps) {
      r.hincrby("key" + (i % 100),"field", i)
    }
  }

  def measure(f: () => Any) = {
    val start = System.currentTimeMillis()
    f()
    val end = System.currentTimeMillis()
    end - start
  }

  def runTest() {
    println("Runnung test " + name + ":  " + nChecks + " runs, " + nIterations + ", iterations in run, " + opsInIteration + " commands in iteration")
    warmup()
    var totalTime: Long = 0
    for (i <- 1 to nChecks;
         j <- 1 to nIterations) {
      resetEnv()
      totalTime += measure(() => withRedis(r => onlyIncrs(r, opsInIteration)))
    }
    println(name + ": " +(totalTime / nChecks))
  }


}
