package com.softwaremill.crawler

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object UsingAkka {

  def crawl(startUrl: Url, http: Http[Future], parseLinks: String => List[Url])(implicit system: ActorSystem): Future[Map[Domain, Int]] = {
    val result = Promise[Map[Domain, Int]]()
    system.actorOf(Props(new Crawler(http, parseLinks, result))) ! Start(startUrl)
    result.future
  }

  class Crawler(http: Http[Future], parseLinks: String => List[Url], result: Promise[Map[Domain, Int]]) extends Actor {
    private var referenceCount = Map[Domain, Int]()
    private var visitedLinks = Set[Url]()
    private var inProgress = Set[Url]()
    private var workers = Map[Domain, ActorRef]()

    override def receive: Receive = {
      case Start(start) =>
        crawlUrl(start)

      case CrawlResult(url, links) =>
        inProgress -= url

        links.foreach { link =>
          crawlUrl(link)
          referenceCount = referenceCount.updated(link.domain, referenceCount.getOrElse(link.domain, 0) + 1)
        }

        if (inProgress.isEmpty) {
          result.success(referenceCount)
          context.stop(self)
        }
    }

    private def crawlUrl(url: Url): Unit = {
      if (!visitedLinks.contains(url)) {
        visitedLinks += url
        inProgress += url
        actorFor(url.domain) ! Crawl(url)
      }
    }

    private def actorFor(domain: Domain): ActorRef = {
      workers.get(domain) match {
        case None =>
          val workerActor = context.actorOf(Props(new Worker(http, parseLinks, self)))
          workers += domain -> workerActor
          workerActor

        case Some(ar) => ar
      }
    }
  }

  class Worker(http: Http[Future], parseLinks: String => List[Url], master: ActorRef) extends Actor with ActorLogging {
    private var urlsPending: Vector[Url] = Vector.empty
    private var getInProgress = false

    override def receive: Receive = {
      case Crawl(url) =>
        urlsPending = urlsPending :+ url
        startHttpGetIfPossible()

      case HttpGetResult(url, Success(body)) =>
        getInProgress = false
        startHttpGetIfPossible()

        val links = parseLinks(body)
        master ! CrawlResult(url, links)

      case HttpGetResult(url, Failure(e)) =>
        getInProgress = false
        startHttpGetIfPossible()

        log.error(s"Cannot get contents of $url", e)
        master ! CrawlResult(url, Nil)
    }

    private def startHttpGetIfPossible(): Unit = {
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

  sealed trait CrawlerMessage
  case class Start(url: Url) extends CrawlerMessage
  case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage

  sealed trait WorkerMessage
  case class Crawl(url: Url) extends WorkerMessage
  case class HttpGetResult(url: Url, result: Try[String]) extends WorkerMessage
}
