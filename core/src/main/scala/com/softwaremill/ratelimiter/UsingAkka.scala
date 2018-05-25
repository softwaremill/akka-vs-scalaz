package com.softwaremill.ratelimiter

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import com.softwaremill.ratelimiter.RateLimiterQueue._

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object UsingAkka {
  class AkkaRateLimiter(rateLimiterActor: ActorRef) extends RateLimiter[Future] {
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
    def create(maxRuns: Int, per: FiniteDuration)(implicit actorSystem: ActorSystem): RateLimiter[Future] = {
      val rateLimiterActor = actorSystem.actorOf(Props(new RateLimiterActor(maxRuns, per)))
      new AkkaRateLimiter(rateLimiterActor)
    }
  }

  private class RateLimiterActor(maxRuns: Int, per: FiniteDuration) extends Actor {

    import context.dispatcher

    private var queue = RateLimiterQueue[LazyFuture](maxRuns, per.toMillis, Queue.empty, Queue.empty, scheduled = false)

    override def receive: Receive = {
      case lf: LazyFuture[Unit] =>
        queue = queue.enqueue(lf)
        pruneAndRun()

      case PruneAndRun =>
        queue = queue.notScheduled
        pruneAndRun()
    }

    private def pruneAndRun(): Unit = {
      val now = System.currentTimeMillis()

      val (tasks, queue2) = queue.pruneAndRun(now)
      queue = queue2
      tasks.foreach {
        case Run(LazyFuture(f)) => f()
        case RunAfter(millis)   => context.system.scheduler.scheduleOnce(millis.millis, self, PruneAndRun)
      }
    }
  }

  private case class LazyFuture[T](t: () => Future[T])
  private case object PruneAndRun
}
