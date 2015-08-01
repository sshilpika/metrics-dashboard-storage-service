package edu.luc.cs.metrics.ingestion.service

import akka.event.Logging
import akka.io.IO
import akka.util.Timeout
import akka.pattern.ask
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http.HttpResponse
import spray.httpx.RequestBuilding._
import concurrent._
import duration._
import ExecutionContext.Implicits._

/**
 * Created by shilpika on 7/24/15.
 */
object rateLimit {

  def calculateRateLimit(accessToken: Option[String]):Future[Int] = {
    implicit val timeout = Timeout(60.seconds)
    Logging(actorsys, getClass)
    val rawHeadersList = accessToken.foldLeft(Nil: List[RawHeader])((list, token) => list:+RawHeader("Authorization", "token "+token))
    val response = (IO(Http) ? Get("https://api.github.com/rate_limit").withHeaders(rawHeadersList)).mapTo[HttpResponse]
    response.map(rateLimit =>{
      rateLimit.headers.filter(x => x.name.equals("X-RateLimit-Remaining"))(0).value.toInt
    })
  }

}
