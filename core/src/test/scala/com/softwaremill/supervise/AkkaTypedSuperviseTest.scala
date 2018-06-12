package com.softwaremill.supervise

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AkkaTypedSuperviseTest
    extends FlatSpec
    with ActorTestKit
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData {

  override def afterAll(): Unit = shutdownTestKit()

  object WrapInFuture extends Wrap[Future] {
    override def apply[T](t: => T): Future[T] = Future { t }
  }

  it should "forward messages and recover from failures" in {
    val testData = createTestData(WrapInFuture)

    val probe = TestProbe[String]()

    val broadcast = spawn(UsingAkkaTyped.broadcastBehavior(testData.queueConnector))
    broadcast ! UsingAkkaTyped.Subscribe(probe.ref)

    probe.expectMessage("msg1")
    probe.expectMessage("msg2")
    probe.expectMessage("msg3")
    probe.expectMessage("msg")
    probe.expectMessage("msg")

    testData.connectingWhileClosing.get() should be(false)
    testData.connectingWithoutClosing.get() should be(false)
  }
}
