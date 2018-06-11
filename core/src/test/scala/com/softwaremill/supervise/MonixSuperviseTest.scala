package com.softwaremill.supervise

import java.util.concurrent.ConcurrentLinkedQueue

import monix.eval.Task
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global

import scala.collection.JavaConverters._

class MonixSuperviseTest
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData
    with Eventually {

  object WrapInMonixTask extends Wrap[Task] {
    override def apply[T](t: => T): Task[T] = Task { t }
  }

  it should "forward messages and recover from failures" in {
    val testData = createTestData(WrapInMonixTask)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val t = for {
      br <- UsingMonix.broadcast(testData.queueConnector)
      _ <- br.inbox.put(UsingMonix.Subscribe(msg => Task.eval(receivedMessages.add(msg))))
    } yield br.cancel

    val cancelBroadcast = t.runSyncUnsafe(1.second)

    try {
      eventually {
        receivedMessages.asScala.toList.slice(0, 5) should be(List("msg1", "msg2", "msg3", "msg", "msg"))

        testData.connectingWhileClosing.get() should be(false)
        testData.connectingWithoutClosing.get() should be(false)
      }
    } finally {
      cancelBroadcast.runAsync

      // get a chance to see that the queue has closed
      Thread.sleep(1000)
    }
  }
}
