package com.softwaremill.supervise

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait SuperviseTestData {
  val closing = new AtomicBoolean()
  val connectingWhileClosing = new AtomicBoolean(false)
  def doClose() = Future {
    closing.set(true)
    Thread.sleep(500)
    closing.set(false)
  }

  val queue1 = new Queue {
    val counter = new AtomicInteger()
    override def take: Future[String] = Future {
      Thread.sleep(100)
      counter.incrementAndGet() match {
        case 1 => "msg1"
        case _ => throw new RuntimeException("exception 1")
      }
    }
    override def close(): Future[Unit] = doClose()
  }
  val queue2 = new Queue {
    val counter = new AtomicInteger()
    override def take: Future[String] = Future {
      Thread.sleep(100)
      counter.incrementAndGet() match {
        case 1 => "msg2"
        case 2 => "msg3"
        case _ => throw new RuntimeException("exception 2")
      }
    }
    override def close(): Future[Unit] = doClose()
  }
  val queue3 = new Queue {
    override def take: Future[String] = Future {
      Thread.sleep(100)
      "msg"
    }
    override def close(): Future[Unit] = doClose()
  }

  val queueConnector = new QueueConnector {
    val counter = new AtomicInteger()
    override def connect: Future[Queue] = Future {
      if (closing.get()) {
        connectingWhileClosing.set(true)
        println(s"Connecting while closing! Counter: ${counter.get()}")
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
