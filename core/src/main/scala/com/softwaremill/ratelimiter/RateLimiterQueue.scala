package com.softwaremill.ratelimiter

import scala.collection.immutable.Queue
import RateLimiterQueue._

case class RateLimiterQueue[F[_]](maxRuns: Int, perMillis: Long, lastTimestamps: Queue[Long], waiting: Queue[F[Unit]], scheduled: Boolean) {

  def pruneAndRun(now: Long): (List[RateLimiterTask[F]], RateLimiterQueue[F]) = {
    pruneTimestamps(now).run(now)
  }

  def enqueue(f: F[Unit]): RateLimiterQueue[F] = copy(waiting = waiting.enqueue(f))

  def notScheduled: RateLimiterQueue[F] = copy(scheduled = false)

  private def run(now: Long): (List[RateLimiterTask[F]], RateLimiterQueue[F]) = {
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

  private def pruneTimestamps(now: Long): RateLimiterQueue[F] = {
    val threshold = now - perMillis
    copy(lastTimestamps = lastTimestamps.filter(_ >= threshold))
  }
}

object RateLimiterQueue {
  sealed trait RateLimiterTask[F[_]]
  case class Run[F[_]](run: F[Unit]) extends RateLimiterTask[F]
  case class RunAfter[F[_]](millis: Long) extends RateLimiterTask[F]
}
