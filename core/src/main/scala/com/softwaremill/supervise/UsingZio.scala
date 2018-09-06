package com.softwaremill.supervise

import com.typesafe.scalalogging.StrictLogging
import scalaz.zio.{Queue => ZIOQueue, _}
import cats.implicits._
import com.softwaremill.IOInstances._

object UsingZio extends StrictLogging {

  sealed trait BroadcastMessage
  case class Subscribe(consumer: String => IO[Nothing, Unit]) extends BroadcastMessage
  case class Received(msg: String) extends BroadcastMessage

  case class BroadcastResult(inbox: ZIOQueue[BroadcastMessage], cancel: IO[Nothing, Unit])

  def broadcast(connector: QueueConnector[IO[Throwable, ?]]): IO[Nothing, BroadcastResult] = {
    def processMessages(inbox: ZIOQueue[BroadcastMessage], consumers: Set[String => IO[Nothing, Unit]]): IO[Nothing, Unit] =
      inbox
        .take
        .flatMap {
          case Subscribe(consumer) => processMessages(inbox, consumers + consumer)
          case Received(msg) =>
            consumers
              .map(consumer => consumer(msg).fork)
              .toList
              .sequence_
              .flatMap(_ => processMessages(inbox, consumers))
        }

    def consumeForever(inbox: ZIOQueue[BroadcastMessage]): IO[Nothing, Unit] =
      consume(connector, inbox).attempt.map {
        case Left(e) =>
          logger.info("[broadcast] exception in queue consumer, restarting", e)
        case Right(()) =>
          logger.info("[broadcast] queue consumer completed, restarting")
      }.forever

    for {
      inbox <- ZIOQueue.bounded[BroadcastMessage](32)
      f1 <- consumeForever(inbox).fork
      f2 <- processMessages(inbox, Set()).fork
    } yield BroadcastResult(inbox, f1.interrupt(new RuntimeException) *> f2.interrupt(new RuntimeException))
  }

  def consume(connector: QueueConnector[IO[Throwable, ?]], inbox: ZIOQueue[BroadcastMessage]): IO[Throwable, Unit] = {
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

    def releaseQueue(queue: Queue[IO[Throwable, ?]]): IO[Nothing, Unit] =
      IO.syncThrowable(logger.info("[queue-stop] closing"))
        .flatMap(_ => queue.close())
        .map(_ => logger.info("[queue-stop] closed"))
        .catchAll(e => IO.now(logger.info("[queue-stop] exception while closing", e)))

    connect.bracket(releaseQueue)(consumeQueue)
  }
}
