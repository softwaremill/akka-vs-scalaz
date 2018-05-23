package com.softwaremill.sockets

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class AkkaSocketTest extends TestKit(ActorSystem("socket-test")) with FlatSpecLike with SocketTest with BeforeAndAfterAll {

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  it should "distribute message and connect new clients" in {
    runTest(socket => UsingAkka.start(socket, system))
  }
}
