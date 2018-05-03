package com.softwaremill

import java.util.concurrent.atomic.AtomicLong

trait Clock {
  def now(): Long
}

object SystemClock extends Clock {
  override def now(): Long = System.currentTimeMillis()
}

class TestClock extends Clock {
  private val storage = new AtomicLong(0L)

  override def now(): Long = storage.get()
  def setNow(now: Long): Unit = storage.set(now)
}
