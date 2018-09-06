package com.softwaremill.crawler

import com.typesafe.scalalogging.StrictLogging
import scalaz.zio.{Fiber, IO, Queue}
import com.softwaremill.IOInstances._
import cats.implicits._

object UsingZio extends StrictLogging {

  def crawl(crawlUrl: Url, http: Http[IO[Throwable, ?]], parseLinks: String => List[Url]): IO[Nothing, Map[Host, Int]] = {

    def crawler(crawlerQueue: Queue[CrawlerMessage], data: CrawlerData): IO[Nothing, Map[Host, Int]] = {
      def handleMessage(msg: CrawlerMessage, data: CrawlerData): IO[Nothing, CrawlerData] = msg match {
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

      def crawlUrl(data: CrawlerData, url: Url): IO[Nothing, CrawlerData] = {
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

      def workerFor(data: CrawlerData, host: Host): IO[Nothing, (CrawlerData, Queue[Url])] = {
        data.workers.get(host) match {
          case None =>
            for {
              workerQueue <- Queue.bounded[Url](32)
              _ <- worker(workerQueue, crawlerQueue)
            } yield {
              (data.copy(workers = data.workers + (host -> workerQueue)), workerQueue)
            }
          case Some(queue) => IO.now((data, queue))
        }
      }

      crawlerQueue.take.flatMap { msg =>
        handleMessage(msg, data).flatMap { data2 =>
          if (data2.inProgress.isEmpty) {
            IO.now(data2.referenceCount)
          } else {
            crawler(crawlerQueue, data2)
          }
        }
      }
    }

    def worker(workerQueue: Queue[Url], crawlerQueue: Queue[CrawlerMessage]): IO[Nothing, Fiber[Nothing, Unit]] = {
      def handleUrl(url: Url): IO[Nothing, Unit] = {
        http
          .get(url)
          .attempt
          .map {
            case Left(t) =>
              logger.error(s"Cannot get contents of $url", t)
              List.empty[Url]
            case Right(b) => parseLinks(b)
          }
          .flatMap(r => crawlerQueue.offer(CrawlResult(url, r)).fork.void)
      }

      workerQueue
        .take
        .flatMap(handleUrl)
        .forever
        .fork
    }

    val crawl = for {
      crawlerQueue <- Queue.bounded[CrawlerMessage](32)
      _ <- crawlerQueue.offer(Start(crawlUrl))
      r <- crawler(crawlerQueue, CrawlerData(Map(), Set(), Set(), Map()))
    } yield r

    IO.supervise(crawl)
  }

  case class CrawlerData(referenceCount: Map[Host, Int], visitedLinks: Set[Url], inProgress: Set[Url], workers: Map[Host, Queue[Url]])

  sealed trait CrawlerMessage
  case class Start(url: Url) extends CrawlerMessage
  case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage
}
