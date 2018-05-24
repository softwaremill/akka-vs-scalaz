package com.softwaremill.supervise

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AkkaSuperviseTest
    extends TestKit(ActorSystem("supervise-test"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData {

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  object WrapInFuture extends Wrap[Future] {
    override def apply[T](t: => T): Future[T] = Future { t }
  }

  it should "forward messages and recover from failures" in {
    val testData = createTestData(WrapInFuture)

    val broadcastActor = system.actorOf(Props(new UsingAkka.BroadcastActor(testData.queueConnector)))
    broadcastActor ! UsingAkka.Subscribe(testActor)

    expectMsg("msg1")
    expectMsg("msg2")
    expectMsg("msg3")
    expectMsg("msg")
    expectMsg("msg")

    testData.connectingWhileClosing.get() should be(false)
    testData.connectingWithoutClosing.get() should be(false)
  }
}
