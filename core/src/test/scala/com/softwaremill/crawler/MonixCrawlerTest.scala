package com.softwaremill.crawler

import monix.eval.Task
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FlatSpec, Matchers}
import monix.execution.Scheduler.Implicits.global
import org.scalatest.time.{Millis, Seconds, Span}

class MonixCrawlerTest extends FlatSpec with Matchers with CrawlerTestData with ScalaFutures with IntegrationPatience {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(60, Seconds)),
      interval = scaled(Span(150, Millis))
    )

  for (testData <- testDataSets) {
    it should s"crawl a test data set ${testData.name}" in {
      import testData._

      val t = timed {
        UsingMonix
          .crawl(startingUrl, url => Task(http(url)), parseLinks)
          .runAsync
          .futureValue should be(expectedCounts)
      }

      shouldTakeMillisMin.foreach(m => t should be >= (m))
      shouldTakeMillisMax.foreach(m => t should be <= (m))
    }
  }
}
