package com.softwaremill.crawler

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class AkkaCrawlerTest
    extends TestKit(ActorSystem("crawler-test"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with CrawlerTestData
    with ScalaFutures {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  it should "crawl a test data set" in {
    UsingAkka.crawl(startingUrl, futureHttp, parseLinks).futureValue should be(expectedCounts)
  }
}
