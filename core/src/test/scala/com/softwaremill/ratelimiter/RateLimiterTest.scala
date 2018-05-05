package com.softwaremill.ratelimiter

import java.time.LocalTime
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import com.softwaremill.ratelimiter.UsingAkka.AkkaRateLimiter
import com.softwaremill.ratelimiter.UsingAkkaTyped.AkkaTypedRateLimiter
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class RateLimiterTest extends FlatSpec with Matchers with BeforeAndAfterAll with Eventually with IntegrationPatience {

  var system: ActorSystem = _

  override protected def beforeAll(): Unit = {
    system = ActorSystem("akka-rate-limiter-test")
  }

  override protected def afterAll(): Unit = {
    system.terminate()
  }

  doTest("AkkaRateLimiter", AkkaRateLimiter.create(_, _)(system))
  doTest("AkkaTypedRateLimiter", AkkaTypedRateLimiter.create)

  def doTest(name: String, create: (Int, FiniteDuration) => RateLimiter[Future]): Unit = {
    name should "rate limit futures scheduled upfront" in {
      val rateLimiter = create(2, 1.second)
      val complete = new AtomicReference(Vector.empty[Int])
      for (i <- 1 to 7) {
        rateLimiter.runLimited(Future {
          println(s"${LocalTime.now()} Running $i")
          complete.updateAndGet(_ :+ i)
        })
      }

      eventually {
        complete.get() should have size (7)
        complete.get().slice(0, 2).toSet should be(Set(1, 2))
        complete.get().slice(2, 4).toSet should be(Set(3, 4))
        complete.get().slice(4, 6).toSet should be(Set(5, 6))
        complete.get().slice(6, 7).toSet should be(Set(7))
      }
    }

    name should "maintain the rate limit in all time windows" in {
      val rateLimiter = create(10, 1.second)
      val complete = new AtomicReference(Vector.empty[Long])
      for (i <- 1 to 20) {
        rateLimiter.runLimited(Future {
          println(s"${LocalTime.now()} Running $i")
          complete.updateAndGet(_ :+ System.currentTimeMillis())
        })
        Thread.sleep(100)
      }

      eventually {
        complete.get() should have size (20)

        // the spacing should be preserved. In a token bucket algorithm, at some point the bucket would refill and
        // all pending futures would be run. In a windowed algorithm, the spacings are preserved.
        val secondHalf = complete.get().slice(10, 20)
        secondHalf.zip(secondHalf.tail).map { case (p, n) => n - p }.foreach { d =>
          d should be <= (150L)
          d should be >= (50L)
        }
      }
    }
  }
}
