package com.softwaremill.supervise

import com.typesafe.scalalogging.StrictLogging
import scalaz.{-\/, \/-}
import scalaz.ioeffect.{IO, Void}

object UsingIOEffect extends StrictLogging {

  def broadcast(connector: QueueConnector[IO[Throwable, ?]], consumer: String => IO[Throwable, Unit]): IO[Nothing, Unit] = {
    consume(connector, consumer).attempt.map {
      case -\/(e) =>
        logger.info("[broadcast] exception in queue consumer, restarting", e)
      case \/-(()) =>
        logger.info("[broadcast] queue consumer completed, restarting")
    }.forever
  }

  def consume(connector: QueueConnector[IO[Throwable, ?]], consumer: String => IO[Throwable, Unit]): IO[Throwable, Unit] = {
    val connect = IO
      .syncThrowable(logger.info("[queue-start] connecting"))
      .flatMap(_ => connector.connect)
      .map { q =>
        logger.info("[queue-start] connected")
        q
      }

    def consumeQueue(queue: Queue[IO[Throwable, ?]]): IO[Throwable, Unit] =
      IO.syncThrowable(logger.info("[queue] receiving message"))
        .flatMap(_ => queue.read())
        .flatMap(consumer)
        .forever

    def releaseQueue(queue: Queue[IO[Throwable, ?]]): IO[Void, Unit] =
      IO.syncThrowable(logger.info("[queue-stop] closing"))
        .flatMap(_ => queue.close())
        .map(_ => logger.info("[queue-stop] closed"))
        .catchAll[Void](e => IO.now(logger.info("[queue-stop] exception while closing", e)))

    connect.bracket(releaseQueue)(consumeQueue)
  }
}
