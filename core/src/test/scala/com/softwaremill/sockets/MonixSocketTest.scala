package com.softwaremill.sockets

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.IntegrationPatience

class MonixSocketTest extends FlatSpec with SocketTest with BeforeAndAfterAll with IntegrationPatience {

  it should "distribute message and connect new clients" in {
    runTest { socket =>
      UsingMonix
        .router(socket)
        .runAsync
    }
  }
}
