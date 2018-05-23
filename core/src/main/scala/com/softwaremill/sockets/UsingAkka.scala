package com.softwaremill.sockets

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

object UsingAkka {

  def start(socket: Socket, system: ActorSystem): ActorRef = {
    system.actorOf(Props(new RouterActor(socket)))
  }

  val Timeout = 10L
  case object Receive
  case class Connected(socket: ConnectedSocket)
  case class Received(socket: ConnectedSocket, msg: String)
  case class Terminated(socket: ConnectedSocket)

  class RouterActor(socket: Socket) extends Actor with ActorLogging {
    case class SocketActors(send: ActorRef, receive: ActorRef)
    private var socketActors = Map[ConnectedSocket, SocketActors]()

    override def preStart(): Unit = {
      context.actorOf(Props(new SocketActor(socket)))
    }

    override def receive: Receive = {
      case Connected(connectedSocket) =>
        val receiveActor = context.actorOf(Props(new ClientReceiveActor(connectedSocket)))
        val sendActor = context.actorOf(Props(new ClientSendActor(connectedSocket)))
        socketActors += connectedSocket -> SocketActors(sendActor, receiveActor)

      case Terminated(connectedSocket) =>
        socketActors.get(connectedSocket).foreach {
          case SocketActors(sendActor, receiveActor) =>
            context.stop(sendActor)
            context.stop(receiveActor)
        }
        socketActors -= connectedSocket

      case Received(receivedFrom, msg) =>
        socketActors.foreach {
          case (connectedSocket, SocketActors(sendActor, _)) =>
            if (receivedFrom != connectedSocket) {
              sendActor ! msg
            }
        }
    }
  }

  class SocketActor(socket: Socket) extends Actor with ActorLogging {

    override def preStart(): Unit = {
      self ! Receive
    }

    override def receive: Receive = {
      case Receive =>
        try {
          val result = socket.accept(Timeout)
          if (result != null) {
            context.parent ! Connected(result)
          }
        } catch {
          case e: Exception =>
            log.error("Exception when listening on a socket", e)
        } finally {
          self ! Receive
        }
    }
  }

  class ClientSendActor(socket: ConnectedSocket) extends Actor with ActorLogging {
    override def receive: Receive = {
      case msg: String =>
        try {
          socket.send(msg)
        } catch {
          case _: SocketTerminatedException =>
            context.parent ! Terminated(socket)
            context.stop(self)
          case e: Exception => log.error(s"Exception when sending $msg", e)
        }
    }
  }

  class ClientReceiveActor(socket: ConnectedSocket) extends Actor with ActorLogging {
    override def preStart(): Unit = {
      self ! Receive
    }

    override def receive: Receive = {
      case Receive =>
        try {
          val msg = socket.receive(Timeout)
          if (msg != null) {
            context.parent ! Received(socket, msg)
          }
        } catch {
          case _: SocketTerminatedException =>
            context.parent ! Terminated(socket)
            context.stop(self)
          case e: Exception =>
            log.error("Exception when receiving from a socket", e)
        } finally {
          self ! Receive
        }
    }
  }
}
