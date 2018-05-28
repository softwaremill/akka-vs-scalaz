package com.softwaremill.ratelimiter

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.IntegrationPatience

import scala.concurrent.duration._

class MonixRateLimiterTest extends RateLimiterTest with IntegrationPatience {

  doTest(
    "monix",
    maxRuns =>
      per =>
        new RateLimiter {
          private val rl = UsingMonix.MonixRateLimiter.create(maxRuns, per).runSyncUnsafe(Duration.Inf)
          override def runLimited(f: => Unit): Unit = rl.runLimited(Task { f }).runAsync
          override def stop(): Unit = rl.stop().runSyncUnsafe(Duration.Inf)
    }
  )
}
