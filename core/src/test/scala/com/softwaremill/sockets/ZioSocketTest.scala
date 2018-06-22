package com.softwaremill.sockets

import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import scalaz.zio.{RTS, Void}

class ZioSocketTest extends FlatSpec with SocketTest with BeforeAndAfterAll with IntegrationPatience with RTS {

  it should "distribute message and connect new clients" in {
    runTest { socket =>
      unsafePerformIOAsync[Void, Unit](UsingZio.router(socket))(_ => ())
    }
  }
}
