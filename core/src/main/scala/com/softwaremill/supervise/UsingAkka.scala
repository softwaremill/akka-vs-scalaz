package com.softwaremill.supervise

import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{
  Actor,
  ActorInitializationException,
  ActorKilledException,
  ActorLogging,
  ActorRef,
  DeathPactException,
  OneForOneStrategy,
  Props,
  SupervisorStrategy
}
import akka.pattern.pipe

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Success

object UsingAkka {

  case class Subscribe(actor: ActorRef)
  case class Received(msg: String)

  // error kernel: parent actor is unaffected by connection problems in the child actor
  class BroadcastActor(connector: QueueConnector[Future]) extends Actor with ActorLogging {
    private var consumers: Set[ActorRef] = Set()

    override def preStart(): Unit = {
      context.actorOf(Props(new ConsumeQueueActor(connector)))
    }

    // optional - the default one is identical
    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
      case _: ActorInitializationException => Stop
      case _: ActorKilledException         => Stop
      case _: DeathPactException           => Stop
      case e: Exception =>
        log.info(s"[broadcast] exception in child actor: ${e.getMessage}, restarting")
        Restart
    }

    override def receive: Receive = {
      case Subscribe(actor) => consumers += actor
      case Received(msg) =>
        consumers.foreach(_ ! msg)
    }
  }

  class ConsumeQueueActor(connector: QueueConnector[Future]) extends Actor with ActorLogging {
    import context.dispatcher

    private var currentQueue: Option[Queue[Future]] = None

    override def preStart(): Unit = {
      log.info("[queue-start] connecting")
      connector.connect.pipeTo(self)
    }

    override def postStop(): Unit = {
      log.info("[queue-stop] stopping queue actor")
      currentQueue.foreach { queue =>
        log.info("[queue-stop] closing")
        Await.result(queue.close(), 1.minute)
        log.info("[queue-stop] closed")
      }
    }

    override def receive: Receive = {
      case queue: Queue[Future] =>
        if (currentQueue.isEmpty) {
          log.info("[queue-start] connected")
          currentQueue = Some(queue)
        }
        log.info("[queue] receiving message")
        queue
          .read()
          .pipeTo(self) // forward message to self
          .andThen { case Success(_) => self ! queue } // receive next message

      case msg: String =>
        context.parent ! Received(msg)

      case Failure(e) =>
        log.info(s"[queue] failure: ${e.getMessage}")
        throw e
    }
  }
}
