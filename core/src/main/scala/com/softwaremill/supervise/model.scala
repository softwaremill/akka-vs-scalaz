package com.softwaremill.supervise

trait Queue[F[_]] {
  def read(): F[String]
  def close(): F[Unit]
}

trait QueueConnector[F[_]] {
  def connect: F[Queue[F]]
}
