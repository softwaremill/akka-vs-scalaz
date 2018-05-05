package com.softwaremill.ratelimiter

import cats.effect.Fiber
import monix.eval.{MVar, Task}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import cats.implicits._

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

  object MonixRateLimiter {
    def create(maxRuns: Int, per: FiniteDuration): Task[MonixRateLimiter] =
      for {
        queue <- MVar.empty[RateLimiterMsg]
        data <- MVar(RateLimiterData(maxRuns, per.toMillis, Queue.empty, Queue.empty, scheduled = false))
        runQueueFiber <- runQueue(data, queue)
      } yield new MonixRateLimiter(queue, runQueueFiber)

    private def runQueue(data: MVar[RateLimiterData], queue: MVar[RateLimiterMsg]): Task[Fiber[Task, Unit]] = {
      queue.take
        .flatMap {
          case PruneAndRun =>
            // we can do take+put here safely because that's the only place where data is accessed
            data.take
              .map(d => d.copy(scheduled = false))
              .flatMap(data.put)
          case Schedule(t) =>
            data.take
              .map(d => d.copy(waiting = d.waiting.enqueue(t)))
              .flatMap(data.put)
        }
        .flatMap { _ =>
          data.take.map(_.pruneAndRun(System.currentTimeMillis())).flatMap {
            case (tasks, d) =>
              data.put(d).map(_ => tasks)
          }
        }
        .flatMap { tasks =>
          tasks
            .map {
              case Run(run)         => run
              case RunAfter(millis) => Task.sleep(millis.millis).flatMap(_ => queue.put(PruneAndRun))
            }
            .map(_.forkAndForget)
            .sequence_
        }
        .restartUntil(_ => false)
        .fork
    }
  }

  private case class RateLimiterData(maxRuns: Int,
                                     perMillis: Long,
                                     lastTimestamps: Queue[Long],
                                     waiting: Queue[Task[Unit]],
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
  private case class Schedule(t: Task[Unit]) extends RateLimiterMsg

  private sealed trait RateLimiterTask
  private case class Run(run: Task[Unit]) extends RateLimiterTask
  private case class RunAfter(millis: Long) extends RateLimiterTask
}
