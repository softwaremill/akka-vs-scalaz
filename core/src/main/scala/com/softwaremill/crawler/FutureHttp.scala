package com.softwaremill.crawler

import scala.concurrent.Future

trait FutureHttp {
  def get(url: String): Future[String]
}
