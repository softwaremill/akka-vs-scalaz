package com.softwaremill.crawler

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import com.softwaremill.crawler.UsingAkkaTyped.Start

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TypedCrawlerTest extends FlatSpec with ActorTestKit with Matchers with BeforeAndAfterAll with CrawlerTestData {

  override def afterAll(): Unit = shutdownTestKit()

  for (testData <- testDataSets) {
    it should s"crawl a test data set ${testData.name}" in {
      import testData._

      val t = timed {
        val probe = TestProbe[Map[String, Int]]()

        val crawler = spawn(new UsingAkkaTyped.Crawler(url => Future(http(url)), parseLinks, probe.ref).crawlerBehavior)
        crawler ! Start(startingUrl)

        probe.expectMessage(1.minute, expectedCounts)
      }
      shouldTakeMillisMin.foreach(m => t should be >= (m))
      shouldTakeMillisMax.foreach(m => t should be <= (m))
    }
  }
}
