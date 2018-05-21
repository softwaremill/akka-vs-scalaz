package com.softwaremill.crawler

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait CrawlerTestData {
  val expectedCounts = Map(
    "d1" -> 6,
    "d2" -> 2,
    "d3" -> 2,
    "d4" -> 2
  )

  val parseLinks: String => List[Url] = (_: String) match {
    case "body11" => List(Url("d1", "p1"), Url("d1", "p2"), Url("d2", "p1"))
    case "body12" => List(Url("d1", "p1"), Url("d1", "p3"), Url("d2", "p1"), Url("d3", "p1"))
    case "body13" => List(Url("d1", "p3"))
    case "body21" => List(Url("d1", "p2"), Url("d3", "p1"), Url("d4", "p1"))
    case "body31" => List(Url("d4", "p1"))
    case "body41" => Nil
  }

  val futureHttp: Http[Future] = (url: Url) =>
    Future(url match {
      case Url("d1", "p1") => "body11"
      case Url("d1", "p2") => "body12"
      case Url("d1", "p3") => "body13"
      case Url("d1", "p4") => "body14"
      case Url("d2", "p1") => "body21"
      case Url("d3", "p1") => "body31"
      case Url("d4", "p1") => "body41"
    })

  val startingUrl = Url("d1", "p1")
}
