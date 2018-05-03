package com.softwaremill.ratelimiter

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorSystem, Behavior}

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object UsingAkkaTyped {
  class AkkaTypedRateLimiter(actorSystem: ActorSystem[RateLimiterMsg]) extends RateLimiter[Future] {
    def runLimited[T](f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]
      actorSystem ! RunFuture(() => f, p)
      p.future
    }

    def stop(): Unit = {
      actorSystem.terminate()
    }
  }

  object AkkaTypedRateLimiter {
    def create(maxRuns: Int, per: FiniteDuration): RateLimiter[Future] = {
      val behavior = Behaviors.withTimers[RateLimiterMsg] { timer =>
        rateLimit(timer, RateLimiterData(maxRuns, per.toMillis, Queue.empty, Queue.empty, scheduled = false))
      }
      new AkkaTypedRateLimiter(ActorSystem(behavior, "rate-limiter"))
    }

    private def rateLimit(timer: TimerScheduler[RateLimiterMsg], data: RateLimiterData): Behavior[RateLimiterMsg] =
      Behaviors.receiveMessage {
        case rf: RunFuture[_] =>
          rateLimit(timer, pruneAndRun(timer, data.copy(waiting = data.waiting.enqueue(rf))))

        case Prune =>
          rateLimit(timer, pruneAndRun(timer, data.copy(scheduled = false)))
      }

    private def pruneAndRun(timer: TimerScheduler[RateLimiterMsg], data: RateLimiterData): RateLimiterData = {
      val now = System.currentTimeMillis()
      run(timer, pruneTimestamps(data, now), now)
    }

    @tailrec
    private def run(timer: TimerScheduler[RateLimiterMsg], data: RateLimiterData, now: Long): RateLimiterData = {
      import data._
      if (lastTimestamps.size < maxRuns) {
        waiting.dequeueOption match {
          case Some((rf, w)) =>
            rf.run()
            run(timer, data.copy(lastTimestamps = lastTimestamps.enqueue(now), waiting = w), now)
          case None =>
            data
        }
      } else if (!scheduled) {
        val nextAvailableSlot = perMillis - (now - lastTimestamps.head)
        timer.startSingleTimer((), Prune, nextAvailableSlot.millis)
        data.copy(scheduled = true)
      } else {
        data
      }
    }

    private def pruneTimestamps(data: RateLimiterData, now: Long): RateLimiterData = {
      val threshold = now - data.perMillis
      data.copy(lastTimestamps = data.lastTimestamps.filter(_ >= threshold))
    }
  }

  private case class RateLimiterData(maxRuns: Int,
                                     perMillis: Long,
                                     lastTimestamps: Queue[Long],
                                     waiting: Queue[RunFuture[_]],
                                     scheduled: Boolean)

  private sealed trait RateLimiterMsg

  private case class RunFuture[T](t: () => Future[T], p: Promise[T])(implicit ec: ExecutionContext) extends RateLimiterMsg {
    def run(): Unit = {
      t().onComplete(r => p.complete(r))
    }
  }

  private case object Prune extends RateLimiterMsg
}
