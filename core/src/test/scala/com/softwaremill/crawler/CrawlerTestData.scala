package com.softwaremill.crawler

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait CrawlerTestData {
  val expectedCounts = Map(
    "url1" -> 3,
    "url2" -> 2,
    "url3" -> 1,
    "url4" -> 1
  )

  val getLinks: String => List[String] = (_: String) match {
    case "body1" => List("url1", "url2")
    case "body2" => List("url2", "url3", "url1")
    case "body3" => List("url4")
    case "body4" => List("url1")
  }

  val futureHttp: FutureHttp = (url: String) =>
    Future(url match {
      case "url1" => "body1"
      case "url2" => "body2"
      case "url3" => "body3"
      case "url4" => "body4"
    })
}
