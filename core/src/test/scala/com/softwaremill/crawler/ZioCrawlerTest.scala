package com.softwaremill.crawler

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}
import scalaz.zio.{IO, RTS}

class ZioCrawlerTest extends FlatSpec with Matchers with CrawlerTestData with ScalaFutures with IntegrationPatience with RTS {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(60, Seconds)),
      interval = scaled(Span(150, Millis))
    )

  for (testData <- testDataSets) {
    it should s"crawl a test data set ${testData.name}" in {
      import testData._

      val t = timed {
        unsafeRun(UsingZio.crawl(startingUrl, url => IO.syncThrowable(http(url)), parseLinks)) should be(expectedCounts)
      }

      shouldTakeMillisMin.foreach(m => t should be >= (m))
      shouldTakeMillisMax.foreach(m => t should be <= (m))
    }
  }
}
