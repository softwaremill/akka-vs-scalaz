package com.softwaremill.supervise

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

trait SuperviseTestData {

  trait TestData[F[_]] {
    def connectingWhileClosing: AtomicBoolean
    def connectingWithoutClosing: AtomicBoolean

    def queue1: Queue[F]
    def queue2: Queue[F]
    def queue3: Queue[F]
    def queueConnector: QueueConnector[F]
  }

  trait Wrap[F[_]] {
    def apply[T](t: => T): F[T]
  }

  def createTestData[F[_]](wrap: Wrap[F]): TestData[F] = new TestData[F] {
    val closing = new AtomicBoolean()
    val lastClosed = new AtomicBoolean(true)
    val connectingWhileClosing = new AtomicBoolean(false)
    val connectingWithoutClosing = new AtomicBoolean(false)

    def doClose() = wrap {
      closing.set(true)
      Thread.sleep(500)
      closing.set(false)
    }

    val queue1: Queue[F] = new Queue[F] {
      val counter = new AtomicInteger()

      override def read(): F[String] = wrap {
        Thread.sleep(500) // delay 1st message so that consumers can subscribe
        counter.incrementAndGet() match {
          case 1 => "msg1"
          case _ => throw new RuntimeException("exception 1")
        }
      }

      override def close(): F[Unit] = doClose()
    }
    val queue2: Queue[F] = new Queue[F] {
      val counter = new AtomicInteger()

      override def read(): F[String] = wrap {
        Thread.sleep(100)
        counter.incrementAndGet() match {
          case 1 => "msg2"
          case 2 => "msg3"
          case _ => throw new RuntimeException("exception 2")
        }
      }

      override def close(): F[Unit] = doClose()
    }
    val queue3: Queue[F] = new Queue[F] {
      override def read(): F[String] = wrap {
        Thread.sleep(100)
        "msg"
      }

      override def close(): F[Unit] = doClose()
    }

    val queueConnector: QueueConnector[F] = new QueueConnector[F] {
      val counter = new AtomicInteger()

      override def connect: F[Queue[F]] = wrap {
        if (closing.get()) {
          connectingWhileClosing.set(true)
          println(s"Connecting while closing! Counter: ${counter.get()}")
        }
        if (!lastClosed.get()) {
          connectingWithoutClosing.set(true)
          println(s"Reconnecting without closing the previous connection! Counter: ${counter.get()}")
        }
        counter.incrementAndGet() match {
          case 1 => queue1
          case 2 => throw new RuntimeException("connect exception 1")
          case 3 => queue2
          case 4 => throw new RuntimeException("connect exception 2")
          case 5 => throw new RuntimeException("connect exception 3")
          case _ => queue3
        }
      }
    }
  }
}
