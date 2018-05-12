package com.softwaremill.crawler

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import com.softwaremill.crawler.UsingAkka.Start
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class CrawlerTest extends TestKit(ActorSystem("crawler-test")) with FlatSpecLike with Matchers with BeforeAndAfterAll with CrawlerTestData {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  it should "crawl a test data set" in {
    val distributor = system.actorOf(Props(new UsingAkka.Crawler(futureHttp, getLinks, testActor)))
    distributor ! Start("url1")

    expectMsg(expectedCounts)
  }
}
