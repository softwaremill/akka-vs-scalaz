package com.softwaremill.ratelimiter

import scalaz._
import Scalaz._
import scalaz.ioeffect.{Fiber, IO, IORef, Promise, Void}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

/*
type Actor[E, I, O] = I => IO[E, O]
 */
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

    def stop(): Unit = {
      runQueueFiber.interrupt(new RuntimeException())
    }
  }

  object IOEffectRateLimiter {
    def create(maxRuns: Int, per: FiniteDuration): IO[Void, IOEffectRateLimiter] =
      for {
        queue <- IOQueue.make[Void, RateLimiterMsg]
        data <- IORef[Void, RateLimiterData](RateLimiterData(maxRuns, per.toMillis, Queue.empty, Queue.empty, scheduled = false))
        runQueueFiber <- runQueue(data, queue)
      } yield new IOEffectRateLimiter(queue, runQueueFiber)

    /*
    why this works: the IORef is only modified when reading from the queue. Hence, there are no race conditions
    to modify the ref data.

    General pattern:
    1 take from queue
    2 read data
    3 modify data, potentially writing to this or other queues
    4 write data

    Unlike in actors, where we have to be cautious not to modify the internal actor state concurrently - e.g. in a
    future callback, here there's no such possibility.
     */
    private def runQueue(data: IORef[RateLimiterData], queue: IOQueue[RateLimiterMsg]): IO[Void, Fiber[Void, Unit]] = {
      queue.take
        .flatMap {
          case PruneAndRun => data.modify(d => d.copy(scheduled = false)).toUnit
          case Schedule(t) => data.modify(d => d.copy(waiting = d.waiting.enqueue(t))).toUnit
        }
        .flatMap { _ =>
          data.modifyFold(_.pruneAndRun(System.currentTimeMillis()))
        }
        .flatMap { tasks =>
          tasks
            .map {
              case Run(run)         => run
              case RunAfter(millis) => IO.sleep[Void](millis.millis).flatMap(_ => queue.offer(PruneAndRun))
            }
            .map(_.fork[Void])
            .sequence_
        }
        .forever
        .fork
    }
  }

  private case class RateLimiterData(maxRuns: Int,
                                     perMillis: Long,
                                     lastTimestamps: Queue[Long],
                                     waiting: Queue[IO[Void, Unit]],
                                     scheduled: Boolean) {

    def pruneAndRun(now: Long): (List[RateLimiterTask], RateLimiterData) = {
      pruneTimestamps(now).run(now)
    }

    private def run(now: Long): (List[RateLimiterTask], RateLimiterData) = {
      if (lastTimestamps.size < maxRuns) {
        waiting.dequeueOption match {
          case Some((io, w)) =>
            val (tasks, next) = copy(lastTimestamps = lastTimestamps.enqueue(now), waiting = w).run(now)
            (Run(io) :: tasks, next)
          case None =>
            (Nil, this)
        }
      } else if (!scheduled) {
        val nextAvailableSlot = perMillis - (now - lastTimestamps.head)
        (List(RunAfter(nextAvailableSlot)), this.copy(scheduled = true))
      } else {
        (Nil, this)
      }
    }

    private def pruneTimestamps(now: Long): RateLimiterData = {
      val threshold = now - perMillis
      copy(lastTimestamps = lastTimestamps.filter(_ >= threshold))
    }
  }

  private sealed trait RateLimiterMsg
  private case object PruneAndRun extends RateLimiterMsg
  private case class Schedule(t: IO[Void, Unit]) extends RateLimiterMsg

  private sealed trait RateLimiterTask
  private case class Run(run: IO[Void, Unit]) extends RateLimiterTask
  private case class RunAfter(millis: Long) extends RateLimiterTask

  // TODO not yet available
  trait IOQueue[T] {
    def take: IO[Void, T]
    def offer[E](t: T): IO[E, Unit]
  }
  object IOQueue {
    def make[E, T]: IO[E, IOQueue[T]] = ???
  }
}
