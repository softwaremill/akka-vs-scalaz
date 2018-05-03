package com.softwaremill.ratelimiter

import scala.concurrent.ExecutionContext

trait RateLimiter[F[_]] {
  def runLimited[T](f: => F[T])(implicit ec: ExecutionContext): F[T]
  def stop(): Unit
}
