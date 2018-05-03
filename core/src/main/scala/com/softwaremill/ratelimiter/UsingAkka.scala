package com.softwaremill.ratelimiter

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Props}
import com.softwaremill.Clock

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

object UsingAkka {
  class AkkaRateLimiter(rateLimiterActor: ActorRef) extends RateLimiter[Future] {
    def runLimited[T](f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]
      rateLimiterActor ! RunFuture(() => f, p)
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

    private val perMillis = per.toMillis

    private var lastTimestamps = Queue.empty[Long]
    private var waiting = Queue.empty[RunFuture[_]]
    private var scheduledPruning = Option.empty[Cancellable]

    override def receive: Receive = {
      case rf: RunFuture[_] =>
        waiting = waiting.enqueue(rf)
        pruneAndRun()

      case Prune =>
        scheduledPruning = None
        pruneAndRun()
    }

    private def pruneAndRun(): Unit = {
      val now = System.currentTimeMillis()
      pruneTimestamps(now)

      @tailrec
      def loop(): Unit = {
        if (lastTimestamps.size < maxRuns) {
          waiting.dequeueOption match {
            case Some((rf, w)) =>
              waiting = w

              rf.run()
              lastTimestamps = lastTimestamps.enqueue(now)

              loop()
            case None =>
          }
        } else if (scheduledPruning.isEmpty) {
          val nextAvailableSlot = perMillis - (now - lastTimestamps.head)
          scheduledPruning = Some(context.system.scheduler.scheduleOnce(nextAvailableSlot.millis, self, Prune))
        }
      }

      loop()
    }

    private def pruneTimestamps(now: Long): Unit = {
      val threshold = now - perMillis
      lastTimestamps = lastTimestamps.filter(_ >= threshold)
    }
  }

  private case class RunFuture[T](t: () => Future[T], p: Promise[T])(implicit ec: ExecutionContext) {
    def run(): Unit = {
      t().onComplete(r => p.complete(r))
    }
  }

  private case object Prune
}
