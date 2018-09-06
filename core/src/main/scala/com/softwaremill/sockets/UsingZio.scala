package com.softwaremill.sockets

import com.typesafe.scalalogging.StrictLogging
import scalaz.zio._
import cats.implicits._
import com.softwaremill.IOInstances._

object UsingZio extends StrictLogging {
  val Timeout = 1000L

  sealed trait RouterMessage
  case class Connected(socket: ConnectedSocket) extends RouterMessage
  case class Received(socket: ConnectedSocket, msg: String) extends RouterMessage
  case class Terminated(socket: ConnectedSocket) extends RouterMessage

  def router(socket: Socket): IO[Nothing, Unit] = {
    case class ConnectedSocketData(sendFiber: Fiber[Unit, Unit], receiveFiber: Fiber[Unit, Unit], sendQueue: Queue[String])
    def handleMessage(queue: Queue[RouterMessage], socketSendQueues: Map[ConnectedSocket, ConnectedSocketData]): IO[Nothing, Unit] = {
      queue.take.flatMap {
        case Connected(connectedSocket) =>
          for {
            sendQueue <- Queue.bounded[String](32)
            sendFiber <- clientSend(connectedSocket, queue, sendQueue)
            receiveFiber <- clientReceive(connectedSocket, queue)
            _ <- handleMessage(queue, socketSendQueues + (connectedSocket -> ConnectedSocketData(sendFiber, receiveFiber, sendQueue)))
          } yield ()

        case Terminated(connectedSocket) =>
          val cancelFibers = socketSendQueues.get(connectedSocket) match {
            case None => IO.unit
            case Some(ConnectedSocketData(sendFiber, receiveFiber, _)) =>
              for {
                _ <- sendFiber.interrupt(new RuntimeException())
                _ <- receiveFiber.interrupt(new RuntimeException())
              } yield ()
          }
          cancelFibers.flatMap(_ => handleMessage(queue, socketSendQueues - connectedSocket))

        case Received(receivedFrom, msg) =>
          val send = socketSendQueues.toList.foldM(()) {
            case (_, (connectedSocket, ConnectedSocketData(_, _, sendQueue))) =>
              if (connectedSocket != receivedFrom) {
                sendQueue.offer(msg)
              } else {
                IO.unit
              }
          }

          send.flatMap(_ => handleMessage(queue, socketSendQueues))
      }
    }

    for {
      queue <- Queue.bounded[RouterMessage](32)
      _ <- socketAccept(socket, queue)
      _ <- handleMessage(queue, Map())
    } yield ()
  }

  def socketAccept(socket: Socket, parent: Queue[RouterMessage]): IO[Nothing, Fiber[Nothing, Unit]] =
    IO.syncThrowable(socket.accept(Timeout))
      .attempt
      .flatMap {
        case Left(e) =>
          logger.error(s"Exception when listening on a socket", e)
          IO.unit
        case Right(null)            => IO.unit
        case Right(connectedSocket) => parent.offer(Connected(connectedSocket))
      }
      .forever
      .fork

  def clientSend(socket: ConnectedSocket, parent: Queue[RouterMessage], sendQueue: Queue[String]): IO[Nothing, Fiber[Unit, Unit]] =
    sendQueue
      .take
      .flatMap(msg => IO.syncThrowable(socket.send(msg)))
      .attempt
      .flatMap {
        case Left(_: SocketTerminatedException) =>
          parent.offer(Terminated(socket)).flatMap(_ => IO.fail[Unit](()))
        case Left(e) =>
          logger.error(s"Exception when sending to socket", e)
          IO.unit
        case Right(_) => IO.unit
      }
      .forever
      .fork

  def clientReceive(socket: ConnectedSocket, parent: Queue[RouterMessage]): IO[Nothing, Fiber[Unit, Unit]] =
    IO.syncThrowable(socket.receive(Timeout))
      .attempt
      .flatMap {
        case Left(_: SocketTerminatedException) =>
          parent.offer(Terminated(socket)).flatMap(_ => IO.fail[Unit](()))
        case Left(e) =>
          logger.error("Exception when receiving from a socket", e)
          IO.unit
        case Right(null) => IO.unit
        case Right(msg)  => parent.offer(Received(socket, msg))
      }
      .forever
      .fork
}
