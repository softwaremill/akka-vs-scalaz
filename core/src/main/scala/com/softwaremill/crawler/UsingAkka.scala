package com.softwaremill.crawler

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object UsingAkka {

  def crawl(startUrl: Url, http: Http[Future], parseLinks: String => List[Url])(implicit system: ActorSystem): Future[Map[Host, Int]] = {
    val result = Promise[Map[Host, Int]]()
    system.actorOf(Props(new Crawler(http, parseLinks, result))) ! Start(startUrl)
    result.future
  }

  class Crawler(http: Http[Future], parseLinks: String => List[Url], result: Promise[Map[Host, Int]]) extends Actor {
    private var referenceCount = Map[Host, Int]()
    private var visitedLinks = Set[Url]()
    private var inProgress = Set[Url]()
    private var workers = Map[Host, ActorRef]()

    override def receive: Receive = {
      case Start(start) =>
        crawlUrl(start)

      case CrawlResult(url, links) =>
        inProgress -= url

        links.foreach { link =>
          crawlUrl(link)
          referenceCount = referenceCount.updated(link.host, referenceCount.getOrElse(link.host, 0) + 1)
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
        actorFor(url.host) ! Crawl(url)
      }
    }

    private def actorFor(host: Host): ActorRef = {
      workers.get(host) match {
        case None =>
          val workerActor = context.actorOf(Props(new Worker(http, parseLinks, self)))
          workers += host -> workerActor
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

  /**
    * Start the crawling process for the given URL. Should be sent only once.
    */
  case class Start(url: Url) extends CrawlerMessage
  case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage

  sealed trait WorkerMessage
  case class Crawl(url: Url) extends WorkerMessage
  case class HttpGetResult(url: Url, result: Try[String]) extends WorkerMessage
}
