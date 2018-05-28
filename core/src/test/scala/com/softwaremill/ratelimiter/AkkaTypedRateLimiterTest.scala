package com.softwaremill.ratelimiter

import com.softwaremill.ratelimiter.UsingAkkaTyped.AkkaTypedRateLimiter
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class AkkaTypedRateLimiterTest extends RateLimiterTest with IntegrationPatience with ScalaFutures {

  doTest(
    "AkkaTypedRateLimiter",
    maxRuns =>
      per =>
        new RateLimiter {
          private val rl = AkkaTypedRateLimiter.create(maxRuns, per)
          override def runLimited(f: => Unit): Unit = rl.runLimited(Future { f })
          override def stop(): Unit = rl.stop().futureValue
    }
  )
}
