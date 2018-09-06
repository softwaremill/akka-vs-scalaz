package com.softwaremill.ratelimiter

import org.scalatest.concurrent.IntegrationPatience
import scalaz.zio.{IO, RTS}

class ZioRateLimiterTest extends RateLimiterTest with IntegrationPatience {

  doTest(
    "zio",
    maxRuns =>
      per =>
        new RateLimiter with RTS {
          private val rl = unsafeRun(UsingZio.ZioRateLimiter.create(maxRuns, per))
          override def runLimited(f: => Unit): Unit =
            unsafeRunAsync(rl.runLimited(IO.syncThrowable(f)))(_ => ())
          override def stop(): Unit = unsafeRun(rl.stop())
    }
  )
}
