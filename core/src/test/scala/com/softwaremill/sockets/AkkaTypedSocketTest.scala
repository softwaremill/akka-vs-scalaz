package com.softwaremill.sockets

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class AkkaTypedSocketTest extends FlatSpec with ActorTestKit with SocketTest with BeforeAndAfterAll {

  override def afterAll(): Unit = shutdownTestKit()

  it should "distribute message and connect new clients" in {
    runTest(socket => spawn(UsingAkkaTyped.routerBehavior(socket)))
  }
}
