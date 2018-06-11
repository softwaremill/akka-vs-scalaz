package com.softwaremill.supervise

import java.util.concurrent.ConcurrentLinkedQueue

import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scalaz.effect.{IO, RTS}

import scala.collection.JavaConverters._

class ZioSuperviseTest
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData
    with Eventually
    with RTS {

  object WrapInZio extends Wrap[IO[Throwable, ?]] {
    override def apply[T](t: => T): IO[Throwable, T] = IO.syncThrowable(t)
  }

  it should "forward messages and recover from failures" in {
    val testData = createTestData(WrapInZio)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val t = for {
      br <- UsingZio.broadcast(testData.queueConnector)
      _ <- br.inbox.offer(UsingZio.Subscribe(msg => IO.sync(receivedMessages.add(msg))))
    } yield br.cancel

    val cancelBroadcast = unsafePerformIO(t)

    try {
      eventually {
        receivedMessages.asScala.toList.slice(0, 5) should be(List("msg1", "msg2", "msg3", "msg", "msg"))

        testData.connectingWhileClosing.get() should be(false)
        testData.connectingWithoutClosing.get() should be(false)
      }
    } finally {
      unsafePerformIOAsync[Nothing, Unit](cancelBroadcast)(_ => ())

      // get a chance to see that the queue has closed
      Thread.sleep(1000)
    }
  }
}
