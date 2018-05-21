package com.softwaremill.crawler

import com.typesafe.scalalogging.StrictLogging
import scalaz._
import Scalaz._
import scalaz.ioeffect.{Fiber, IO, IORef}

object UsingIOEffect extends StrictLogging {

  def crawler(crawlUrl: Url, http: Http[IO[Throwable, ?]], parseLinks: String => List[Url]): IO[Nothing, Map[Domain, Int]] = {

    case class CrawlerData(referenceCount: Map[Domain, Int],
                           visitedLinks: Set[Url],
                           inProgress: Set[Url],
                           workers: Map[Domain, IOQueue[WorkerMessage]])

    case class WorkerData(
        urlsPending: Vector[Url],
        getInProgress: Boolean
    )

    def crawler(crawlerQueue: IOQueue[CrawlerMessage], data: CrawlerData): IO[Nothing, Map[Domain, Int]] = {
      def handleMessage(msg: CrawlerMessage, data: CrawlerData): IO[Nothing, CrawlerData] = msg match {
        case Start(url) =>
          crawlUrl(data, url)

        case CrawlResult(url, links) =>
          val data2 = data.copy(inProgress = data.inProgress - url)

          links.foldlM(data2) { d => link =>
            val d2 = d.copy(referenceCount = d.referenceCount.updated(link.domain, d.referenceCount.getOrElse(link.domain, 0) + 1))
            crawlUrl(d2, link)
          }
      }

      def crawlUrl(data: CrawlerData, url: Url): IO[Nothing, CrawlerData] = {
        if (!data.visitedLinks.contains(url)) {
          workerFor(data, url.domain).flatMap {
            case (data2, workerQueue) =>
              workerQueue.offer(Crawl(url)).map { _ =>
                data2.copy(
                  visitedLinks = data.visitedLinks + url,
                  inProgress = data.inProgress + url
                )
              }
          }
        } else IO.now(data)
      }

      def workerFor(data: CrawlerData, url: Domain): IO[Nothing, (CrawlerData, IOQueue[WorkerMessage])] = {
        data.workers.get(url) match {
          case None =>
            for {
              workerQueue <- IOQueue.make[Nothing, WorkerMessage]
              _ <- worker(workerQueue, crawlerQueue)
            } yield {
              (data.copy(workers = data.workers + (url -> workerQueue)), workerQueue)
            }
          case Some(wq) => IO.now((data, wq))
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

    def worker(workerQueue: IOQueue[WorkerMessage], crawlerQueue: IOQueue[CrawlerMessage]): IO[Nothing, Fiber[Nothing, Unit]] = {
      def handleMessage(msg: WorkerMessage, data: WorkerData): IO[Nothing, WorkerData] =
        msg match {
          case Crawl(url) =>
            startHttpGetIfPossible(data.copy(urlsPending = data.urlsPending :+ url))

          case HttpGetResult(url, result) =>
            val links = result.fold({ t =>
              logger.error(s"Cannot get contents of $url", t)
              List.empty[Url]
            }, parseLinks)

            crawlerQueue.offer[Nothing](CrawlResult(url, links)).flatMap(_ => startHttpGetIfPossible(data))
        }

      def startHttpGetIfPossible(data: WorkerData): IO[Nothing, WorkerData] =
        data.urlsPending match {
          case url +: tail if !data.getInProgress =>
            val httpGet = http.get(url).attempt[Nothing].flatMap(r => workerQueue.offer(HttpGetResult(url, r))).fork
            httpGet.map(_ => data.copy(urlsPending = tail, getInProgress = true))

          case _ =>
            IO.now(data)
        }

      IORef[Nothing, WorkerData](WorkerData(Vector.empty, getInProgress = false)).flatMap { data =>
        workerQueue.take
          .flatMap(msg => data.read.map((msg, _)))
          .flatMap((handleMessage _).tupled)
          .flatMap(data.write)
          .forever
          .fork
      }
    }

    sealed trait CrawlerMessage
    case class Start(url: Url) extends CrawlerMessage
    case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage

    sealed trait WorkerMessage
    case class Crawl(url: Url) extends WorkerMessage
    case class HttpGetResult(url: Url, result: Throwable \/ String) extends WorkerMessage

    for {
      crawlerQueue <- IOQueue.make[Nothing, CrawlerMessage]
      _ <- crawlerQueue.offer[Nothing](Start(crawlUrl))
      r <- crawler(crawlerQueue, CrawlerData(Map(), Set(), Set(), Map()))
      // TODO: stop fibers; unlike in Akka, child fibers aren't automatically stopped
    } yield r
  }

  // TODO not yet available
  trait IOQueue[T] {
    def take: IO[Nothing, T]
    def offer[E](t: T): IO[E, Unit]
  }
  object IOQueue {
    def make[E, T]: IO[E, IOQueue[T]] = ???
  }
}
