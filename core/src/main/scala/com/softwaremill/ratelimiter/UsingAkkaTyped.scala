package com.softwaremill.ratelimiter

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorSystem, Behavior}
import com.softwaremill.ratelimiter.RateLimiterQueue.{Run, RunAfter}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object UsingAkkaTyped {
  class AkkaTypedRateLimiter(actorSystem: ActorSystem[RateLimiterMsg]) extends StrictLogging {
    def runLimited[T](f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]
      actorSystem ! LazyFuture(() => f.andThen { case r => p.complete(r) }.map(_ => ()))
      p.future
    }

    def stop(): Future[Unit] = {
      import actorSystem.executionContext
      actorSystem.terminate().map { _ =>
        // the actor system's logger is no longer available
        logger.info("Stopping rate limiter")
      }
    }
  }

  object AkkaTypedRateLimiter {
    def create(maxRuns: Int, per: FiniteDuration): AkkaTypedRateLimiter = {
      val behavior = Behaviors.withTimers[RateLimiterMsg] { timer =>
        rateLimit(timer, RateLimiterQueue(maxRuns, per.toMillis))
      }
      new AkkaTypedRateLimiter(ActorSystem(behavior, "rate-limiter"))
    }

    private def rateLimit(timer: TimerScheduler[RateLimiterMsg], data: RateLimiterQueue[LazyFuture]): Behavior[RateLimiterMsg] =
      Behaviors.receiveMessage {
        case lf: LazyFuture    => rateLimit(timer, runQueue(timer, data.enqueue(lf)))
        case ScheduledRunQueue => rateLimit(timer, runQueue(timer, data.notScheduled))
      }

    private def runQueue(timer: TimerScheduler[RateLimiterMsg], data: RateLimiterQueue[LazyFuture]): RateLimiterQueue[LazyFuture] = {
      val now = System.currentTimeMillis()

      val (tasks, data2) = data.run(now)
      tasks.foreach {
        case Run(LazyFuture(f)) => f()
        case RunAfter(millis)   => timer.startSingleTimer((), ScheduledRunQueue, millis.millis)
      }

      data2
    }
  }

  private sealed trait RateLimiterMsg
  private case class LazyFuture(t: () => Future[Unit]) extends RateLimiterMsg
  private case object ScheduledRunQueue extends RateLimiterMsg
}
