package edu.luc.cs.metrics.ingestion.service

import java.time.Instant
import akka.io.IO
import akka.util.Timeout
import com.mongodb.casbah.MongoDB
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http.HttpResponse
import spray.httpx.RequestBuilding._
import concurrent.duration._
import akka.pattern.ask
import spray.json.JsValue
import concurrent.ExecutionContext.Implicits._
/**
 * Created by shilpika on 7/24/15.
 */

sealed trait Metric
case class Issues(issuesList:List[JsValue], db: MongoDB) extends Metric
case class Commits(commitList:List[JsValue], db: MongoDB) extends Metric

trait Ingestion {
  def timeout(time: FiniteDuration): Timeout  = Timeout(time)

  def url(urlStr:String, page:Option[String]): String = if(page.isDefined) urlStr+"&"+page.get else urlStr

  def rawHeaderList(accessToken:Option[String]): List[RawHeader] =
    accessToken.foldLeft(Nil: List[RawHeader])((list, token) => list:+RawHeader("Authorization", "token "+token))

  def getHttpResponse(url: String, rawHeadersList: List[RawHeader], time: FiniteDuration) = {
    implicit val timeOut = timeout(time)
    (IO(Http) ? Get(url).withHeaders(rawHeadersList)).mapTo[HttpResponse]
  }

}

trait ingestionStrategy{

  def mongoCasbah(dbName:String) = mongoClientCasbah(dbName)

  def getNextPage(gitList: HttpResponse): Option[String] = {
    println("This is a list of headers:")
    val link = gitList.headers.filter(x => x.name.equals("Link"))
    gitList.headers.map(x => println(x.name + "!!!!!!!!!!!!!!!!" + x.value))
    println(link)
    val nextUrlForCurrentWeek = if (!link.isEmpty)
      Option(link(0)).flatMap(x => {
        val i = x.value.indexOf("page")
        val page = x.value.substring(i).split(">")(0)
        if (page.equals("page=1")) None else Some(page)
      })
    else None
    nextUrlForCurrentWeek
  }

  def rateLimitCheck(gitList: HttpResponse): Unit = {
    val rateTime = gitList.headers.filter(x => x.name.equals("X-RateLimit-Reset"))(0).value
    val rateRemaining = gitList.headers.filter(x => x.name.equals("X-RateLimit-Remaining"))(0).value
    val inst = Instant.ofEpochSecond(rateTime.toLong)
    if (rateRemaining == 0) {
      println("Sleeping Rate Limit = 0")
      Thread.sleep(inst.toEpochMilli)
    }
    else if (rateRemaining.toInt % 1000 == 0) {
      Thread.sleep(60000)
    }
  }


}

trait CommitKlocIngestion{

  def reactiveMongoDb(dbName: String) = connection.db(dbName)

}


