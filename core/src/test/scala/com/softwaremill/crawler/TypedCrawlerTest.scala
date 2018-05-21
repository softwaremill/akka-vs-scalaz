package com.softwaremill.crawler

import akka.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import com.softwaremill.crawler.UsingAkkaTyped.Start
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TypedCrawlerTest extends FlatSpec with ActorTestKit with Matchers with BeforeAndAfterAll with CrawlerTestData {

  override def afterAll(): Unit = shutdownTestKit()

  it should "crawl a test data set" in {
    val probe = TestProbe[Map[String, Int]]()

    val crawler = spawn(new UsingAkkaTyped.Crawler(futureHttp, parseLinks, probe.ref).crawlerBehavior)
    crawler ! Start(startingUrl)

    probe.expectMessage(expectedCounts)
  }
}
