package com.softwaremill.supervise

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.{Consumer, Observable}
import monix.execution.Scheduler.Implicits.global

object UsingMonix extends StrictLogging {

  def broadcast(connector: QueueConnector[Task], consumer: Consumer[String, Unit]): Task[Unit] = {
    consume(connector, consumer).attempt
      .map {
        case Left(e) =>
          logger.info("[broadcast] exception in queue consumer, restarting", e)
        case Right(()) =>
          logger.info("[broadcast] queue consumer completed, restarting")
      }
      .restartUntil(_ => false)
      .map(_ => logger.info("[broadcast] finished"))
  }

  def consume(connector: QueueConnector[Task], consumer: Consumer[String, Unit]): Task[Unit] = {
    val connect = Task
      .eval(logger.info("[queue-start] connecting"))
      .flatMap(_ => connector.connect)
      .map { q =>
        logger.info("[queue-start] connected")
        q
      }

    def consumeQueue(queue: Queue[Task]) =
      Observable
        .repeatEvalF(Task.eval(logger.info("[queue] receiving message")).flatMap(_ => queue.read()))
        .consumeWith(consumer)

    def releaseQueue(queue: Queue[Task]) =
      Task
        .eval(logger.info("[queue-stop] closing"))
        .flatMap(_ => queue.close())
        .map(_ => logger.info("[queue-stop] closed"))

    connect.bracket(consumeQueue)(releaseQueue)
  }
}
