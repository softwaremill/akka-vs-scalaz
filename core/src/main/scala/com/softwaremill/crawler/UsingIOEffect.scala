package com.softwaremill.crawler

import com.typesafe.scalalogging.StrictLogging
import scalaz._
import Scalaz._
import scalaz.ioeffect.{Fiber, IO, Void}

object UsingIOEffect extends StrictLogging {

  def crawler(crawlUrl: Url, http: Http[IO[Throwable, ?]], parseLinks: String => List[Url]): IO[Void, Map[Domain, Int]] = {

    def crawler(crawlerQueue: IOQueue[CrawlerMessage], data: CrawlerData): IO[Void, Map[Domain, Int]] = {
      def handleMessage(msg: CrawlerMessage, data: CrawlerData): IO[Void, CrawlerData] = msg match {
        case Start(url) =>
          crawlUrl(data, url)

        case CrawlResult(url, links) =>
          val data2 = data.copy(inProgress = data.inProgress - url)

          links.foldlM(data2) { d => link =>
            val d2 = d.copy(referenceCount = d.referenceCount.updated(link.domain, d.referenceCount.getOrElse(link.domain, 0) + 1))
            crawlUrl(d2, link)
          }
      }

      def crawlUrl(data: CrawlerData, url: Url): IO[Void, CrawlerData] = {
        if (!data.visitedLinks.contains(url)) {
          workerFor(data, url.domain).flatMap {
            case (data2, workerQueue) =>
              workerQueue.offer(url).map { _ =>
                data2.copy(
                  visitedLinks = data.visitedLinks + url,
                  inProgress = data.inProgress + url
                )
              }
          }
        } else IO.now(data)
      }

      def workerFor(data: CrawlerData, url: Domain): IO[Void, (CrawlerData, IOQueue[Url])] = {
        data.workers.get(url) match {
          case None =>
            for {
              workerQueue <- IOQueue.make[Void, Url]
              workerFiber <- worker(workerQueue, crawlerQueue)
            } yield {
              (data.copy(workers = data.workers + (url -> WorkerData(workerQueue, workerFiber))), workerQueue)
            }
          case Some(wd) => IO.now((data, wd.queue))
        }
      }

      crawlerQueue.take.flatMap { msg =>
        handleMessage(msg, data).flatMap { data2 =>
          if (data2.inProgress.isEmpty) {
            data2.workers.values.map(_.fiber.interrupt[Void](new RuntimeException())).toList.sequence_.map(_ => data2.referenceCount)
          } else {
            crawler(crawlerQueue, data2)
          }
        }
      }
    }

    def worker(workerQueue: IOQueue[Url], crawlerQueue: IOQueue[CrawlerMessage]): IO[Void, Fiber[Void, Unit]] = {
      def handleUrl(url: Url): IO[Void, Unit] = {
        http
          .get(url)
          .attempt
          .map {
            case -\/(t) =>
              logger.error(s"Cannot get contents of $url", t)
              List.empty[Url]
            case \/-(b) => parseLinks(b)
          }
          .flatMap(r => crawlerQueue.offer(CrawlResult(url, r)))
      }

      workerQueue.take
        .flatMap(handleUrl)
        .forever
        .fork
    }

    for {
      crawlerQueue <- IOQueue.make[Void, CrawlerMessage]
      _ <- crawlerQueue.offer[Void](Start(crawlUrl))
      r <- crawler(crawlerQueue, CrawlerData(Map(), Set(), Set(), Map()))
    } yield r
  }

  case class WorkerData(queue: IOQueue[Url], fiber: Fiber[Void, Unit])
  case class CrawlerData(referenceCount: Map[Domain, Int], visitedLinks: Set[Url], inProgress: Set[Url], workers: Map[Domain, WorkerData])

  sealed trait CrawlerMessage
  case class Start(url: Url) extends CrawlerMessage
  case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage

  // TODO not yet available
  trait IOQueue[T] {
    def take: IO[Void, T]
    def offer[E](t: T): IO[E, Unit]
  }
  object IOQueue {
    def make[E, T]: IO[E, IOQueue[T]] = ???
  }
}
