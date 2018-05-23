package com.softwaremill.supervise

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class AkkaSuperviseTest
    extends TestKit(ActorSystem("supervise-test"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData {

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  it should "forward messages and recover from failures" in {
    val broadcastActor = system.actorOf(Props(new UsingAkka.BroadcastActor(queueConnector)))
    broadcastActor ! UsingAkka.Subscribe(testActor)

    expectMsg("msg1")
    expectMsg("msg2")
    expectMsg("msg3")
    expectMsg("msg")
    expectMsg("msg")

    connectingWhileClosing.get() should be(false)
    connectingWithoutClosing.get() should be(false)
  }
}
