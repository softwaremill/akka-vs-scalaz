package com.softwaremill.ratelimiter

import cats.effect.Fiber
import monix.eval.{MVar, Task}

import scala.concurrent.duration._
import cats.implicits._
import RateLimiterQueue._
import com.typesafe.scalalogging.StrictLogging

object UsingMonix {
  class MonixRateLimiter(queue: MVar[RateLimiterMsg], queueFiber: Fiber[Task, Unit]) {
    def runLimited[T](f: Task[T]): Task[T] = {
      for {
        mv <- MVar.empty[T]
        _ <- queue.put(Schedule(f.flatMap(mv.put)))
        r <- mv.take
      } yield r
    }

    def stop(): Task[Unit] = {
      queueFiber.cancel
    }
  }

  object MonixRateLimiter extends StrictLogging {
    def create(maxRuns: Int, per: FiniteDuration): Task[MonixRateLimiter] =
      for {
        queue <- MVar.empty[RateLimiterMsg]
        runQueueFiber <- runQueue(RateLimiterQueue[Task[Unit]](maxRuns, per.toMillis), queue)
          .doOnCancel(Task.eval(logger.info("Stopping rate limiter")))
          .fork
      } yield new MonixRateLimiter(queue, runQueueFiber)

    private def runQueue(data: RateLimiterQueue[Task[Unit]], queue: MVar[RateLimiterMsg]): Task[Unit] = {
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
                case RunAfter(millis) => Task.sleep(millis.millis).flatMap(_ => queue.put(ScheduledRunQueue))
              }
              .map(_.forkAndForget)
              .sequence_
              .map(_ => d)
        }
        .flatMap(d => runQueue(d, queue))
    }
  }

  private sealed trait RateLimiterMsg
  private case object ScheduledRunQueue extends RateLimiterMsg
  private case class Schedule(t: Task[Unit]) extends RateLimiterMsg
}
