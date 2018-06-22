package com.softwaremill.supervise

import com.typesafe.scalalogging.StrictLogging
import scalaz.zio._
import cats.implicits._
import com.softwaremill.IOInstances._

object UsingZio extends StrictLogging {

  sealed trait BroadcastMessage
  case class Subscribe(consumer: String => IO[Void, Unit]) extends BroadcastMessage
  case class Received(msg: String) extends BroadcastMessage

  case class BroadcastResult(inbox: IOQueue[BroadcastMessage], cancel: IO[Void, Unit])

  def broadcast(connector: QueueConnector[IO[Throwable, ?]]): IO[Void, BroadcastResult] = {
    def processMessages(inbox: IOQueue[BroadcastMessage], consumers: Set[String => IO[Void, Unit]]): IO[Void, Unit] =
      inbox
        .take[Void]
        .flatMap {
          case Subscribe(consumer) => processMessages(inbox, consumers + consumer)
          case Received(msg) =>
            consumers
              .map(consumer => consumer(msg).fork[Void])
              .toList
              .sequence_
              .flatMap(_ => processMessages(inbox, consumers))
        }

    def consumeForever(inbox: IOQueue[BroadcastMessage]): IO[Void, Unit] =
      consume(connector, inbox).attempt.map {
        case Left(e) =>
          logger.info("[broadcast] exception in queue consumer, restarting", e)
        case Right(()) =>
          logger.info("[broadcast] queue consumer completed, restarting")
      }.forever

    for {
      inbox <- IOQueue.make[Void, BroadcastMessage](32)
      f1 <- consumeForever(inbox).fork
      f2 <- processMessages(inbox, Set()).fork
    } yield BroadcastResult(inbox, f1.interrupt(new RuntimeException) *> f2.interrupt(new RuntimeException))
  }

  def consume(connector: QueueConnector[IO[Throwable, ?]], inbox: IOQueue[BroadcastMessage]): IO[Throwable, Unit] = {
    val connect: IO[Throwable, Queue[IO[Throwable, ?]]] = IO
      .syncThrowable(logger.info("[queue-start] connecting"))
      .flatMap(_ => connector.connect)
      .map { q =>
        logger.info("[queue-start] connected")
        q
      }

    def consumeQueue(queue: Queue[IO[Throwable, ?]]): IO[Throwable, Unit] =
      IO.syncThrowable(logger.info("[queue] receiving message"))
        .flatMap(_ => queue.read())
        .flatMap(msg => inbox.offer(Received(msg)))
        .forever

    def releaseQueue(queue: Queue[IO[Throwable, ?]]): IO[Void, Unit] =
      IO.syncThrowable(logger.info("[queue-stop] closing"))
        .flatMap(_ => queue.close())
        .map(_ => logger.info("[queue-stop] closed"))
        .catchAll[Void](e => IO.now(logger.info("[queue-stop] exception while closing", e)))

    connect.bracket(releaseQueue)(consumeQueue)
  }
}
