package com.softwaremill.sockets

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object UsingAkkaTyped {
  val Timeout = 10L
  case object Receive

  sealed trait RouterMessage
  case class Connected(socket: ConnectedSocket) extends RouterMessage
  case class Received(socket: ConnectedSocket, msg: String) extends RouterMessage
  case class Terminated(socket: ConnectedSocket) extends RouterMessage

  def routerBehavior(socket: Socket): Behavior[RouterMessage] = Behaviors.setup[RouterMessage] { ctx =>
    ctx.spawn[Nothing](socketBehavior(socket, ctx.self), "socket")

    case class SocketActors(send: ActorRef[String], receive: ActorRef[Nothing])
    def doReceive(socketActors: Map[ConnectedSocket, SocketActors]): Behavior[RouterMessage] =
      Behaviors.receiveMessage {
        case Connected(connectedSocket) =>
          val receiveActor = ctx.spawn[Nothing](clientReceiveBehavior(connectedSocket, ctx.self), s"receive-$connectedSocket")
          val sendActor = ctx.spawn(clientSendBehavior(connectedSocket, ctx.self), s"send-$connectedSocket")
          doReceive(socketActors + (connectedSocket -> SocketActors(sendActor, receiveActor)))

        case Terminated(connectedSocket) =>
          socketActors.get(connectedSocket).foreach {
            case SocketActors(sendActor, receiveActor) =>
              ctx.stop(sendActor)
              ctx.stop(receiveActor)
          }
          doReceive(socketActors - connectedSocket)

        case Received(receivedFrom, msg) =>
          socketActors.foreach {
            case (connectedSocket, SocketActors(sendActor, _)) =>
              if (receivedFrom != connectedSocket) {
                sendActor ! msg
              }
          }

          doReceive(socketActors)
      }

    doReceive(Map())
  }

  def socketBehavior(socket: Socket, parent: ActorRef[Connected]): Behavior[Nothing] =
    Behaviors
      .setup[Receive.type] { ctx =>
        ctx.self ! Receive

        Behaviors.receiveMessage {
          case Receive =>
            try {
              val result = socket.accept(Timeout)
              if (result != null) {
                parent ! Connected(result)
              }
            } catch {
              case e: Exception =>
                ctx.log.error("Exception when listening on a socket", e)
            } finally {
              ctx.self ! Receive
            }

            Behaviors.same
        }
      }
      .narrow[Nothing]

  def clientSendBehavior(socket: ConnectedSocket, parent: ActorRef[Terminated]): Behavior[String] = Behaviors.setup { ctx =>
    Behaviors.receiveMessage { msg: String =>
      try {
        socket.send(msg)
        Behaviors.same
      } catch {
        case _: SocketTerminatedException =>
          parent ! Terminated(socket)
          Behavior.stopped
        case e: Exception =>
          ctx.log.error(s"Exception when sending $msg", e)
          Behaviors.same
      }
    }
  }

  def clientReceiveBehavior(socket: ConnectedSocket, parent: ActorRef[RouterMessage]): Behavior[Nothing] =
    Behaviors
      .setup[Receive.type] { ctx =>
        ctx.self ! Receive

        Behaviors.receiveMessage {
          case Receive =>
            try {
              val msg = socket.receive(Timeout)
              if (msg != null) {
                parent ! Received(socket, msg)
              }
              Behaviors.same
            } catch {
              case _: SocketTerminatedException =>
                parent ! Terminated(socket)
                Behavior.stopped
              case e: Exception =>
                ctx.log.error("Exception when receiving from a socket", e)
                Behaviors.same
            } finally {
              ctx.self ! Receive
            }
        }
      }
      .narrow[Nothing]
}
