package com.softwaremill.crawler

import com.typesafe.scalalogging.StrictLogging
import scalaz.zio.{Fiber, IO, IOQueue}
import com.softwaremill.IOInstances._
import cats.implicits._

object UsingZio extends StrictLogging {

  def crawl(crawlUrl: Url, http: Http[IO[Throwable, ?]], parseLinks: String => List[Url]): IO[Void, Map[Host, Int]] = {

    def crawler(crawlerQueue: IOQueue[CrawlerMessage], data: CrawlerData): IO[Void, Map[Host, Int]] = {
      def handleMessage(msg: CrawlerMessage, data: CrawlerData): IO[Void, CrawlerData] = msg match {
        case Start(url) =>
          crawlUrl(data, url)

        case CrawlResult(url, links) =>
          val data2 = data.copy(inProgress = data.inProgress - url)

          links.foldM(data2) {
            case (d, link) =>
              val d2 = d.copy(referenceCount = d.referenceCount.updated(link.host, d.referenceCount.getOrElse(link.host, 0) + 1))
              crawlUrl(d2, link)
          }
      }

      def crawlUrl(data: CrawlerData, url: Url): IO[Void, CrawlerData] = {
        if (!data.visitedLinks.contains(url)) {
          workerFor(data, url.host).flatMap {
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

      def workerFor(data: CrawlerData, host: Host): IO[Void, (CrawlerData, IOQueue[Url])] = {
        data.workers.get(host) match {
          case None =>
            for {
              workerQueue <- IOQueue.make[Void, Url](32)
              _ <- worker(workerQueue, crawlerQueue)
            } yield {
              (data.copy(workers = data.workers + (host -> workerQueue)), workerQueue)
            }
          case Some(queue) => IO.now((data, queue))
        }
      }

      crawlerQueue.take[Void].flatMap { msg =>
        handleMessage(msg, data).flatMap { data2 =>
          if (data2.inProgress.isEmpty) {
            IO.now(data2.referenceCount)
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
            case Left(t) =>
              logger.error(s"Cannot get contents of $url", t)
              List.empty[Url]
            case Right(b) => parseLinks(b)
          }
          .flatMap(r => crawlerQueue.offer(CrawlResult(url, r)).fork[Void].toUnit)
      }

      workerQueue
        .take[Void]
        .flatMap(handleUrl)
        .forever
        .fork
    }

    val crawl = for {
      crawlerQueue <- IOQueue.make[Void, CrawlerMessage](32)
      _ <- crawlerQueue.offer[Void](Start(crawlUrl))
      r <- crawler(crawlerQueue, CrawlerData(Map(), Set(), Set(), Map()))
    } yield r

    IO.supervise(crawl, new RuntimeException)
  }

  case class CrawlerData(referenceCount: Map[Host, Int], visitedLinks: Set[Url], inProgress: Set[Url], workers: Map[Host, IOQueue[Url]])

  sealed trait CrawlerMessage
  case class Start(url: Url) extends CrawlerMessage
  case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage
}
