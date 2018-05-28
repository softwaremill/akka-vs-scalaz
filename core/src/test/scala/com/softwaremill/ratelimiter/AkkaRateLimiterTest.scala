package com.softwaremill.ratelimiter

import akka.actor.ActorSystem
import com.softwaremill.ratelimiter.UsingAkka.AkkaRateLimiter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.IntegrationPatience

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class AkkaRateLimiterTest extends RateLimiterTest with BeforeAndAfterAll with IntegrationPatience {

  var system: ActorSystem = _

  override protected def beforeAll(): Unit = {
    system = ActorSystem("akka-rate-limiter-test")
  }

  override protected def afterAll(): Unit = {
    system.terminate()
  }

  doTest(
    "AkkaRateLimiter",
    maxRuns =>
      per =>
        new RateLimiter {
          private val rl = AkkaRateLimiter.create(maxRuns, per)(system)
          override def runLimited(f: => Unit): Unit = rl.runLimited(Future { f })
          override def stop(): Unit = rl.stop()
    }
  )
}
