package com.softwaremill.ratelimiter

import scalaz._
import Scalaz._
import scalaz.ioeffect.{Fiber, IO, Promise, Void}

import scala.concurrent.duration._
import RateLimiterQueue._
import com.typesafe.scalalogging.StrictLogging

object UsingIOEffect {
  class IOEffectRateLimiter(queue: IOQueue[RateLimiterMsg], runQueueFiber: Fiber[Void, Unit]) {
    def runLimited[E, T](f: IO[E, T]): IO[E, T] = {
      for {
        p <- Promise.make[E, T]
        toRun = f.flatMap(p.complete).catchAll[Void](p.error).fork[Void].toUnit
        _ <- queue.offer[E](Schedule(toRun))
        r <- p.get
      } yield r
    }

    def stop(): IO[Nothing, Unit] = {
      runQueueFiber.interrupt(new StopException())
    }
  }

  object IOEffectRateLimiter extends StrictLogging {

    def create(maxRuns: Int, per: FiniteDuration): IO[Void, IOEffectRateLimiter] =
      for {
        queue <- IOQueue.make[Void, RateLimiterMsg]
        runQueueFiber <- runQueue(RateLimiterQueue(maxRuns, per.toMillis), queue)
          .ensuring(IO.sync(logger.info("Stopping rate limiter")))
          .fork
      } yield new IOEffectRateLimiter(queue, runQueueFiber)

    private def runQueue(data: RateLimiterQueue[IO[Void, Unit]], queue: IOQueue[RateLimiterMsg]): IO[Void, Unit] = {
      queue.take
        .map {
          case ScheduledRunQueue => data.notScheduled
          case Schedule(t)       => data.enqueue(t)
        }
        .map(_.run(System.currentTimeMillis()))
        .flatMap {
          case (tasks, d) =>
            tasks
              .map {
                case Run(run)         => run
                case RunAfter(millis) => IO.sleep[Void](millis.millis).flatMap(_ => queue.offer(ScheduledRunQueue))
              }
              .map(_.fork[Void])
              .sequence_
              .map(_ => d)
        }
        .flatMap(d => runQueue(d, queue))
    }
  }

  private sealed trait RateLimiterMsg
  private case object ScheduledRunQueue extends RateLimiterMsg
  private case class Schedule(t: IO[Void, Unit]) extends RateLimiterMsg

  private class StopException extends RuntimeException

  // TODO not yet available
  trait IOQueue[T] {
    def take: IO[Void, T]
    def offer[E](t: T): IO[E, Unit]
  }
  object IOQueue {
    def make[E, T]: IO[E, IOQueue[T]] = ???
  }
}
