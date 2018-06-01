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
        data <- MVar(RateLimiterQueue[Task[Unit]](maxRuns, per.toMillis))
        runQueueFiber <- runQueue(data, queue)
      } yield new MonixRateLimiter(queue, runQueueFiber)

    private def runQueue(data: MVar[RateLimiterQueue[Task[Unit]]], queue: MVar[RateLimiterMsg]): Task[Fiber[Task, Unit]] = {
      queue.take
        .flatMap {
          case ScheduledRunQueue =>
            // we can do take+put here safely because that's the only place where data is accessed
            data.take
              .map(d => d.notScheduled)
              .flatMap(data.put)
          case Schedule(t) =>
            data.take
              .map(_.enqueue(t))
              .flatMap(data.put)
        }
        .flatMap { _ =>
          data.take.map(_.run(System.currentTimeMillis())).flatMap {
            case (tasks, d) =>
              data.put(d).map(_ => tasks)
          }
        }
        .flatMap { tasks =>
          tasks
            .map {
              case Run(run)         => run
              case RunAfter(millis) => Task.sleep(millis.millis).flatMap(_ => queue.put(ScheduledRunQueue))
            }
            .map(_.forkAndForget)
            .sequence_
        }
        .restartUntil(_ => false)
        .doOnCancel(Task.eval(logger.info("Stopping rate limiter")))
        .fork
    }
  }

  private sealed trait RateLimiterMsg
  private case object ScheduledRunQueue extends RateLimiterMsg
  private case class Schedule(t: Task[Unit]) extends RateLimiterMsg
}
