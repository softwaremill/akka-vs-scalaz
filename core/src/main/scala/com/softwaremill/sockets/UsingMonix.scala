package com.softwaremill.sockets

import com.typesafe.scalalogging.StrictLogging
import monix.eval.{Fiber, Task}
import monix.execution.misc.AsyncQueue
import cats.implicits._

object UsingMonix extends StrictLogging {
  val Timeout = 10L

  sealed trait RouterMessage
  case class Connected(socket: ConnectedSocket) extends RouterMessage
  case class Received(socket: ConnectedSocket, msg: String) extends RouterMessage
  case class Terminated(socket: ConnectedSocket) extends RouterMessage

  def router(socket: Socket): Task[Unit] = {
    case class ConnectedSocketData(sendFiber: Fiber[Unit], receiveFiber: Fiber[Unit], sendQueue: MQueue[String])
    def handleMessage(queue: MQueue[RouterMessage], socketSendQueues: Map[ConnectedSocket, ConnectedSocketData]): Task[Unit] = {
      queue.take.flatMap {
        case Connected(connectedSocket) =>
          val sendQueue = MQueue.make[String]
          for {
            sendFiber <- clientSend(connectedSocket, queue, sendQueue)
            receiveFiber <- clientReceive(connectedSocket, queue)
            _ <- handleMessage(queue, socketSendQueues + (connectedSocket -> ConnectedSocketData(sendFiber, receiveFiber, sendQueue)))
          } yield ()

        case Terminated(connectedSocket) =>
          val cancelFibers = socketSendQueues.get(connectedSocket) match {
            case None => Task.now(())
            case Some(ConnectedSocketData(sendFiber, receiveFiber, _)) =>
              for {
                _ <- sendFiber.cancel
                _ <- receiveFiber.cancel
              } yield ()
          }
          cancelFibers.flatMap(_ => handleMessage(queue, socketSendQueues - connectedSocket))

        case Received(receivedFrom, msg) =>
          val send = socketSendQueues.toList.foldM(()) {
            case (_, (connectedSocket, ConnectedSocketData(_, _, sendQueue))) =>
              if (connectedSocket != receivedFrom) {
                sendQueue.offer(msg)
              } else {
                Task.now(())
              }
          }

          send.flatMap(_ => handleMessage(queue, socketSendQueues))
      }
    }

    val queue = MQueue.make[RouterMessage]
    socketAccept(socket, queue).flatMap { _ =>
      handleMessage(queue, Map())
    }
  }

  def socketAccept(socket: Socket, parent: MQueue[RouterMessage]): Task[Fiber[Unit]] =
    Task
      .eval(socket.accept(Timeout))
      .flatMap {
        case null            => Task.now(())
        case connectedSocket => parent.offer(Connected(connectedSocket))
      }
      .onErrorRecover {
        case e: Exception =>
          logger.error(s"Exception when listening on a socket", e)
      }
      .restartUntil(_ => false)
      .fork

  def clientSend(socket: ConnectedSocket, parent: MQueue[RouterMessage], sendQueue: MQueue[String]): Task[Fiber[Unit]] =
    sendQueue.take
      .flatMap(msg => Task.eval(socket.send(msg)))
      .map(_ => false)
      .onErrorRecoverWith {
        case _: SocketTerminatedException => parent.offer(Terminated(socket)).map(_ => true)
        case e: Exception =>
          logger.error(s"Exception when sending to socket", e)
          Task.now(false)
      }
      .restartUntil(identity)
      .map(_ => ())
      .fork

  def clientReceive(socket: ConnectedSocket, parent: MQueue[RouterMessage]): Task[Fiber[Unit]] =
    Task
      .eval(socket.receive(Timeout))
      .flatMap {
        case null => Task.now(())
        case msg  => parent.offer(Received(socket, msg))
      }
      .map(_ => false)
      .onErrorRecoverWith {
        case _: SocketTerminatedException => parent.offer(Terminated(socket)).map(_ => true)
        case e: Exception =>
          logger.error("Exception when receiving from a socket", e)
          Task.now(false)
      }
      .restartUntil(identity)
      .map(_ => ())
      .fork

  //

  class MQueue[T](q: AsyncQueue[T]) {
    def take: Task[T] = {
      Task.deferFuture(q.poll())
    }
    def offer(t: T): Task[Unit] = {
      Task.eval(q.offer(t))
    }
  }
  object MQueue {
    def make[T]: MQueue[T] = new MQueue(AsyncQueue.empty)
  }
}
