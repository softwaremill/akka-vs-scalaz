package com.softwaremill.ratelimiter

import scalaz.zio.{Fiber, IO, Queue, Promise}

import scala.concurrent.duration._
import RateLimiterQueue._
import com.typesafe.scalalogging.StrictLogging
import cats.implicits._
import com.softwaremill.IOInstances._

object UsingZio {
  class ZioRateLimiter(queue: Queue[RateLimiterMsg], runQueueFiber: Fiber[Nothing, Unit]) {
    def runLimited[E, T](f: IO[E, T]): IO[E, T] = {
      for {
        p <- Promise.make[E, T]
        toRun = f.flatMap(p.complete).catchAll(p.error).fork.void
        _ <- queue.offer(Schedule(toRun))
        r <- p.get
      } yield r
    }

    def stop(): IO[Nothing, Unit] = {
      runQueueFiber.interrupt(new StopException())
    }
  }

  object ZioRateLimiter extends StrictLogging {

    def create(maxRuns: Int, per: FiniteDuration): IO[Nothing, ZioRateLimiter] =
      for {
        queue <- Queue.bounded[RateLimiterMsg](32)
        runQueueFiber <- runQueue(RateLimiterQueue(maxRuns, per.toMillis), queue)
          .ensuring(IO.sync(logger.info("Stopping rate limiter")))
          .fork
      } yield new ZioRateLimiter(queue, runQueueFiber)

    private def runQueue(data: RateLimiterQueue[IO[Nothing, Unit]], queue: Queue[RateLimiterMsg]): IO[Nothing, Unit] = {
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
            // (4) convert each rate-limiter-task to an IO
              .map {
                case Run(run)         => run
                case RunAfter(millis) => IO.sleep(millis.millis).flatMap(_ => queue.offer(ScheduledRunQueue))
              }
              // (5) fork each converted IO so that it runs in the background
              .map(_.fork)
              // (6) sequence a list of IOs which spawn background fibers
              // into one big IO which, when run, will spawn all of them
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
  private case class Schedule(t: IO[Nothing, Unit]) extends RateLimiterMsg

  private class StopException extends RuntimeException
}
