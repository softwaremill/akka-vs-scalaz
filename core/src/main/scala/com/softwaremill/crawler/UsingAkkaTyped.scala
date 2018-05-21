package com.softwaremill.crawler

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object UsingAkkaTyped {
  def crawler(http: Http[Future], getLinks: String => List[String], reportTo: ActorRef[Map[String, Int]]): Behavior[CrawlerMessage] =
    Behaviors.setup[CrawlerMessage] { ctx =>
      case class CrawlerData(referenceCount: Map[String, Int],
                             visitedLinks: Set[String],
                             inProgress: Set[String],
                             workers: Map[String, ActorRef[WorkerMessage]])

      def crawlUrl(data: CrawlerData, url: String): CrawlerData = {
        if (!data.visitedLinks.contains(url)) {
          val (data2, worker) = workerFor(data, url)
          worker ! Crawl(url)
          data2.copy(
            visitedLinks = data.visitedLinks + url,
            inProgress = data.inProgress + url
          )
        } else data
      }

      def workerFor(data: CrawlerData, url: String): (CrawlerData, ActorRef[WorkerMessage]) = {
        data.workers.get(url) match {
          case None =>
            val ar = ctx.spawn(workerReceive(ctx.self, Vector.empty, getInProgress = false), s"worker-$url")
            (data.copy(workers = data.workers + (url -> ar)), ar)

          case Some(ar) => (data, ar)
        }
      }

      def crawlerReceive(data: CrawlerData): Behavior[CrawlerMessage] = {
        Behaviors.receiveMessage {
          case Start(start) =>
            crawlerReceive(crawlUrl(data, start))

          case CrawlResult(url, links) =>
            val data2 = data.copy(inProgress = data.inProgress - url)

            val data3 = links.foldLeft(data2) {
              case (d, link) =>
                val d2 = d.copy(referenceCount = d.referenceCount.updated(link, d.referenceCount.getOrElse(link, 0) + 1))
                crawlUrl(d2, link)
            }

            if (data3.inProgress.isEmpty) {
              reportTo ! data3.referenceCount
              Behavior.stopped
            } else {
              crawlerReceive(data3)
            }
        }
      }

      def workerReceive(master: ActorRef[CrawlResult], urlsPending: Vector[String], getInProgress: Boolean): Behavior[WorkerMessage] =
        Behaviors.receiveMessage {
          case Crawl(url) =>
            workerStartHttpGetIfPossible(master, urlsPending :+ url, getInProgress)

          case HttpGetResult(url, Success(body)) =>
            val links = getLinks(body)
            master ! CrawlResult(url, links)

            workerStartHttpGetIfPossible(master, urlsPending, getInProgress = false)

          case HttpGetResult(url, Failure(e)) =>
            ctx.log.error(s"Cannot get contents of $url", e)
            master ! CrawlResult(url, Nil)

            workerStartHttpGetIfPossible(master, urlsPending, getInProgress = false)
        }

      def workerStartHttpGetIfPossible(master: ActorRef[CrawlResult],
                                       urlsPending: Vector[String],
                                       getInProgress: Boolean): Behavior[WorkerMessage] =
        Behaviors.setup[WorkerMessage] { ctx =>
          urlsPending match {
            case url +: tail if !getInProgress =>
              import ctx.executionContext
              http.get(url).onComplete(r => ctx.self ! HttpGetResult(url, r))

              workerReceive(master, tail, getInProgress = true)

            case _ =>
              workerReceive(master, urlsPending, getInProgress)
          }
        }

      crawlerReceive(CrawlerData(Map(), Set(), Set(), Map()))
    }

  sealed trait WorkerMessage
  case class Crawl(url: String) extends WorkerMessage
  case class HttpGetResult(url: String, result: Try[String]) extends WorkerMessage

  sealed trait CrawlerMessage
  case class Start(url: String) extends CrawlerMessage
  case class CrawlResult(url: String, links: List[String]) extends CrawlerMessage
}
