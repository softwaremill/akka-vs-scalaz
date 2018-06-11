package com.softwaremill.supervise

import com.typesafe.scalalogging.StrictLogging
import monix.eval.{MVar, Task}
import cats.implicits._

object UsingMonix extends StrictLogging {

  sealed trait BroadcastMessage
  case class Subscribe(consumer: String => Task[Unit]) extends BroadcastMessage
  case class Received(msg: String) extends BroadcastMessage

  case class BroadcastResult(inbox: MVar[BroadcastMessage], cancel: Task[Unit])

  def broadcast(connector: QueueConnector[Task]): Task[BroadcastResult] = {
    def processMessages(inbox: MVar[BroadcastMessage], consumers: Set[String => Task[Unit]]): Task[Unit] =
      inbox.take
        .flatMap {
          case Subscribe(consumer) => processMessages(inbox, consumers + consumer)
          case Received(msg) =>
            consumers
              .map(consumer => consumer(msg).fork)
              .toList
              .sequence_
              .flatMap(_ => processMessages(inbox, consumers))
        }

    def consumeForever(inbox: MVar[BroadcastMessage]): Task[Unit] =
      consume(connector, inbox).attempt
        .map {
          case Left(e) =>
            logger.info("[broadcast] exception in queue consumer, restarting", e)
          case Right(()) =>
            logger.info("[broadcast] queue consumer completed, restarting")
        }
        .restartUntil(_ => false)

    for {
      inbox <- MVar.empty[BroadcastMessage]
      f1 <- consumeForever(inbox).fork
      f2 <- processMessages(inbox, Set()).fork
    } yield BroadcastResult(inbox, f1.cancel *> f2.cancel)
  }

  def consume(connector: QueueConnector[Task], inbox: MVar[BroadcastMessage]): Task[Unit] = {
    val connect: Task[Queue[Task]] = Task
      .eval(logger.info("[queue-start] connecting"))
      .flatMap(_ => connector.connect)
      .map { q =>
        logger.info("[queue-start] connected")
        q
      }

    def consumeQueue(queue: Queue[Task]): Task[Unit] =
      Task
        .eval(logger.info("[queue] receiving message"))
        .flatMap(_ => queue.read())
        .flatMap(msg => inbox.put(Received(msg)))
        .cancelable
        .restartUntil(_ => false)

    def releaseQueue(queue: Queue[Task]): Task[Unit] =
      Task
        .eval(logger.info("[queue-stop] closing"))
        .flatMap(_ => queue.close())
        .map(_ => logger.info("[queue-stop] closed"))

    connect.bracket(consumeQueue)(releaseQueue)
  }
}
