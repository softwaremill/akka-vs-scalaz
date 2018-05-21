package com.softwaremill.crawler

trait Http[F[_]] {
  def get(url: String): F[String]
}
