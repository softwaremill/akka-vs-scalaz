package com.softwaremill.crawler

trait Http[F[_]] {
  def get(url: Url): F[String]
}
