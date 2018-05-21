package com.softwaremill.crawler

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object UsingAkka {
  class Crawler(http: Http[Future], getLinks: String => List[String], reportTo: ActorRef) extends Actor {
    private var referenceCount = Map[String, Int]()
    private var visitedLinks = Set[String]()
    private var inProgress = Set[String]()
    private var workers = Map[String, ActorRef]()

    override def receive: Receive = {
      case Start(start) =>
        crawlUrl(start)

      case CrawlResult(url, links) =>
        inProgress -= url

        links.foreach { link =>
          crawlUrl(link)
          referenceCount = referenceCount.updated(link, referenceCount.getOrElse(link, 0) + 1)
        }

        if (inProgress.isEmpty) {
          reportTo ! referenceCount
          context.stop(self)
        }
    }

    private def crawlUrl(url: String): Unit = {
      if (!visitedLinks.contains(url)) {
        visitedLinks += url
        inProgress += url
        actorFor(url) ! Crawl(url)
      }
    }

    private def actorFor(url: String): ActorRef = {
      workers.get(url) match {
        case None =>
          val ar = context.actorOf(Props(new Worker(http, getLinks, self)))
          workers += url -> ar
          ar

        case Some(ar) => ar
      }
    }
  }

  class Worker(http: Http[Future], getLinks: String => List[String], master: ActorRef) extends Actor with ActorLogging {
    private var urlsPending: Vector[String] = Vector.empty
    private var getInProgress = false

    override def receive: Receive = {
      case Crawl(url) =>
        urlsPending = urlsPending :+ url
        startGetIfPossible()

      case HttpGetResult(url, Success(body)) =>
        getInProgress = false
        startGetIfPossible()

        val links = getLinks(body)
        master ! CrawlResult(url, links)

      case HttpGetResult(url, Failure(e)) =>
        getInProgress = false
        startGetIfPossible()

        log.error(s"Cannot get contents of $url", e)
        master ! CrawlResult(url, Nil)
    }

    private def startGetIfPossible(): Unit = {
      urlsPending match {
        case url +: tail if !getInProgress =>
          getInProgress = true
          urlsPending = tail

          import context.dispatcher
          http.get(url).onComplete(r => self ! HttpGetResult(url, r))

        case _ =>
      }
    }
  }

  sealed trait WorkerMessage
  case class Crawl(url: String) extends WorkerMessage
  case class HttpGetResult(url: String, result: Try[String]) extends WorkerMessage

  sealed trait CrawlerMessage
  case class Start(url: String) extends CrawlerMessage
  case class CrawlResult(url: String, links: List[String]) extends CrawlerMessage
}
