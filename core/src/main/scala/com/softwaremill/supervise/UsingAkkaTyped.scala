package com.softwaremill.supervise

import akka.actor.typed.{ActorRef, Behavior, PostStop, SupervisorStrategy, Terminated}
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object UsingAkkaTyped {

  sealed trait BroadcastActorMessage
  case class Subscribe(actor: ActorRef[String]) extends BroadcastActorMessage
  case class Received(msg: String) extends BroadcastActorMessage

  def broadcastBehavior(connector: QueueConnector[Future]): Behavior[BroadcastActorMessage] = Behaviors.setup { ctx =>
    val connectBehavior = Behaviors
      .supervise[Nothing](connectToQueueBehavior(connector, ctx.self))
      .onFailure[RuntimeException](SupervisorStrategy.restart)
    ctx.spawn[Nothing](connectBehavior, "connect-queue")

    def handleMessage(consumers: Set[ActorRef[String]]): Behavior[BroadcastActorMessage] = Behaviors.receiveMessage {
      case Subscribe(actor) => handleMessage(consumers + actor)
      case Received(msg) =>
        consumers.foreach(_ ! msg)
        handleMessage(consumers)
    }

    handleMessage(Set())
  }

  def connectToQueueBehavior(connector: QueueConnector[Future], msgSink: ActorRef[Received]): Behavior[Nothing] = {
    Behaviors.setup[Try[Queue[Future]]] { ctx =>
      import ctx.executionContext

      ctx.log.info("[queue-start] connecting")
      connector.connect.andThen { case result => ctx.self ! result }

      Behaviors.receiveMessage {
        case Success(queue) =>
          ctx.log.info("[queue-start] connected")

          val consumeActor = ctx.spawn(consumeQueueBehavior(queue, msgSink), "consume-queue")
          ctx.watch(consumeActor)

          // we can either not handle Terminated, which will cause DeathPactException to be thrown and propagated
          // or rethrow the original exception
          Behaviors.receiveSignal {
            case (_, t @ Terminated(_)) =>
              t.failure.foreach(throw _)
              Behaviors.empty
          }
        case Failure(e) =>
          ctx.log.info("[queue-start] failure")
          throw e
      }
    }
  }.narrow[Nothing]

  /*
  By default actors are stopped on error: https://doc.akka.io/docs/akka/current/typed/fault-tolerance.html
  as opposed to normal actors, which are restarted (defaultDecider)
   */

  def consumeQueueBehavior(queue: Queue[Future], msgSink: ActorRef[Received]): Behavior[Try[String]] =
    Behaviors.setup { ctx =>
      import ctx.executionContext

      ctx.log.info("[queue] receiving message")
      queue.read().andThen { case result => ctx.self ! result }

      Behaviors
        .receiveMessage[Try[String]] {
          case Success(msg) =>
            msgSink ! Received(msg)
            consumeQueueBehavior(queue, msgSink)

          case Failure(e) =>
            ctx.log.info(s"[queue] failure: ${e.getMessage}")
            throw e
        }
        .receiveSignal {
          case (_, PostStop) =>
            ctx.log.info("[queue-stop] closing")
            Await.result(queue.close(), 1.minute)
            ctx.log.info("[queue-stop] closed")
            Behaviors.same
        }
    }
}
