package com.softwaremill.supervise

import scala.concurrent.Future

trait Queue {
  def take: Future[String]
  def close(): Future[Unit]
}

trait QueueConnector {
  def connect: Future[Queue]
}
