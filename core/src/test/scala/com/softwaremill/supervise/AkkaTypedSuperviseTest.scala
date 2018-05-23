package com.softwaremill.supervise

import akka.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AkkaTypedSuperviseTest
    extends FlatSpec
    with ActorTestKit
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData {

  override def afterAll(): Unit = shutdownTestKit()

  it should "forward messages and recover from failures" in {

    val probe = TestProbe[String]()

    val broadcast = spawn(UsingAkkaTyped.broadcastBehavior(queueConnector))
    broadcast ! UsingAkkaTyped.Subscribe(probe.ref)

    probe.expectMessage("msg1")
    probe.expectMessage("msg2")
    probe.expectMessage("msg3")
    probe.expectMessage("msg")
    probe.expectMessage("msg")

    connectingWhileClosing.get() should be(false)
    connectingWithoutClosing.get() should be(false)
  }
}
