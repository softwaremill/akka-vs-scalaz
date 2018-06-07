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
      queue
      // (1) take a message from the queue (or wait until one is available)
      .take
      // (2) modify the data structure accordingly
        .map {
          case ScheduledRunQueue => data.notScheduled
          case Schedule(t)       => data.enqueue(t)
        }
        // (3) run the rate limiter queue: obtain the rate-limiter-tasks to be run
        .map(_.run(System.currentTimeMillis()))
        .flatMap {
          case (tasks, d) =>
            tasks
            // (4) convert each rate-limiter-task to a Monix-Task
              .map {
                case Run(run)         => run
                case RunAfter(millis) => Task.sleep(millis.millis).flatMap(_ => queue.put(ScheduledRunQueue))
              }
              // (5) fork each converted Monix-Task so that it runs in the background
              .map(_.fork)
              // (6) sequence a list of tasks which spawn background fibers
              // into one big task which, when run, will spawn all of them
              .sequence_
              .map(_ => d)
        }
        // (7) recursive call to handle the next message,
        // using the updated data structure
        .flatMap(d => runQueue(d, queue))
    }
  }

  private sealed trait RateLimiterMsg
  private case object ScheduledRunQueue extends RateLimiterMsg
  private case class Schedule(t: Task[Unit]) extends RateLimiterMsg
}
