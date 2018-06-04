package com.softwaremill.ratelimiter

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import com.softwaremill.ratelimiter.RateLimiterQueue._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object UsingAkka {
  class AkkaRateLimiter(rateLimiterActor: ActorRef) {
    def runLimited[T](f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]
      rateLimiterActor ! LazyFuture(() => f.andThen { case r => p.complete(r) }.map(_ => ()))
      p.future
    }

    def stop(): Unit = {
      rateLimiterActor ! PoisonPill
    }
  }

  object AkkaRateLimiter {
    def create(maxRuns: Int, per: FiniteDuration)(implicit actorSystem: ActorSystem): AkkaRateLimiter = {
      val rateLimiterActor = actorSystem.actorOf(Props(new RateLimiterActor(maxRuns, per)))
      new AkkaRateLimiter(rateLimiterActor)
    }
  }

  private class RateLimiterActor(maxRuns: Int, per: FiniteDuration) extends Actor with ActorLogging {

    import context.dispatcher

    // mutable actor state: the current rate limiter queue; the queue itself is immutable, but the reference
    // is mutable and access to it is protected by the actor.
    private var queue = RateLimiterQueue[LazyFuture](maxRuns, per.toMillis)

    override def receive: Receive = {
      case lf: LazyFuture =>
        // enqueueing the new computation and checking if any computations can be run
        queue = queue.enqueue(lf)
        runQueue()

      case ScheduledRunQueue =>
        // clearing the `scheduled` flag, as we are in a scheduled run right now, so it's possible a new one
        // has to be scheduled
        queue = queue.notScheduled
        runQueue()
    }

    private def runQueue(): Unit = {
      val now = System.currentTimeMillis()

      val (tasks, queue2) = queue.run(now)
      // Updating the mutable reference to store the new queue.
      queue = queue2
      // Each task returned by `queue.run` is turned into a side-effect: either running the lazy future
      // (which amounts to running the block of code which creates the future - and hence makes the
      // computation run), or scheduling a `ScheduledRunQueue` message to be sent to the actor after
      // the given delay.
      tasks.foreach {
        case Run(LazyFuture(f)) => f()
        case RunAfter(millis)   => context.system.scheduler.scheduleOnce(millis.millis, self, ScheduledRunQueue)
      }
    }

    override def postStop(): Unit = {
      log.info("Stopping rate limiter")
    }
  }

  private case class LazyFuture(t: () => Future[Unit])
  private case object ScheduledRunQueue
}
