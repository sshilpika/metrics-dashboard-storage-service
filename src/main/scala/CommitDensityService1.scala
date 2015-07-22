import java.time.temporal.TemporalAdjusters

import akka.actor.{ActorSystem}
import akka.event.Logging
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http._
import spray.httpx.RequestBuilding._
import spray.json._
import DefaultJsonProtocol._
import util._
import concurrent.duration._
import concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Await, Future}
import com.mongodb.casbah.Imports._
import java.time._
/**
 * Created by sshilpika on 6/30/15.
 */

object Connection{
  val mongoClient = MongoClient("localhost", 27017)

  val system = ActorSystem("gitCommitDensity")

}

object CommitUrl{

  def getRepoCommitUrl(user: String, repo: String, branch:String, accessToken: Option[String], page: Option[String]): Future[String] ={

    implicit val system = Connection.system
    val log = Logging(system, getClass)
    implicit val timeout = Timeout(60.seconds)
    val rawHeadersList = accessToken.foldLeft(Nil: List[RawHeader])((list, token) => list:+RawHeader("Authorization", "token "+token))
    val url = if(page.isDefined){"https://api.github.com/repos/"+user+"/"+repo+"/commits?sha="+branch+"&"+page.get} else "https://api.github.com/repos/"+user+"/"+repo+"/commits?sha="+branch
    val gitFileCommitList =
      (IO(Http) ? Get(url).withHeaders(rawHeadersList)).mapTo[HttpResponse]

    val nextUrlForCurrentWeek = gitFileCommitList.map(gitList => {
      println("This is a list of headers:")
      val link = gitList.headers.filter(x => x.name.equals("Link"))
      gitList.headers.map(x => println(x.name+ "!!!!!!!!!!!!!!!!" +x.value))
      println(link)

      val rateTime =  gitList.headers.filter(x => x.name.equals("X-RateLimit-Reset"))(0).value
      val rateRemaining = gitList.headers.filter(x => x.name.equals("X-RateLimit-Remaining"))(0).value
      val inst = Instant.ofEpochSecond(rateTime.toLong)
      if(rateRemaining == 0){
        println("Sleeping Rate Limit = 0")
        Thread.sleep(inst.toEpochMilli)
      }
      else if(rateRemaining.toInt%1000 == 0){
        Thread.sleep(60000)
      }

      val nextUrlForCurrentWeek = if(!link.isEmpty)
        Option(link(0)).flatMap(x =>{
        val i = x.value.indexOf("page")
        val page = x.value.substring(i).split(">")(0)
        if(page.equals("page=1")) None else Some(page)
      })else None

      //extracting urls from the commit info obtained and storing urlList in the DB
      val mongoClient = Connection.mongoClient
      val db = mongoClient(user+"_"+repo+"_"+branch+"_"+"URL")
      gitList.entity.data.asString.parseJson.convertTo[List[JsValue]].map(commits => {

        val date = commits.asJsObject.getFields("commit")(0).asJsObject.getFields("committer")(0).asJsObject.getFields("date")(0).compactPrint.replaceAll("\"", "")
        val ldt = ZonedDateTime.ofInstant(Instant.parse(date),ZoneId.of("UTC"))
        val start_date = Instant.parse(date).minus(java.time.Duration.ofDays(ldt.getDayOfWeek.getValue-1))
        val since = ZonedDateTime.ofInstant(start_date,ZoneId.of("UTC"))
        val coll = db("COLL"+ldt.getYear+"DAY"+since.getDayOfYear)
        val url = commits.asJsObject.getFields("url")(0).compactPrint.replaceAll("\"","")
        //inserts if the date is not present else all the matched documents are updated with multi update set to true
        coll.update(MongoDBObject("date" -> date),$set("url"-> url, "date" -> date),true,true)

      })
      Thread.sleep(20000)
      //the url for next page of commits
      println("nextUrlForCurrentWeek: "+nextUrlForCurrentWeek)
      nextUrlForCurrentWeek
    })


    nextUrlForCurrentWeek.flatMap(nextUrl => nextUrl.map(page =>{
      getRepoCommitUrl(user,repo,branch,accessToken,Option(page))
    } ).getOrElse(Future("")))

  }
}


object CommitUrlCollection extends App{

  val lines = scala.io.Source.stdin.getLines
  println("Enter username/reponame/branchname")
  val input = lines.next().split("/")

  println(s"You entered: \nUsername: ${input(0)} \nReponame: ${input(1)} \nBranchname: ${input(2)}\n")

  println("Enter Authentication Token:")
  val token = lines.next()
  println("TOKEN: "+token)

  val f = CommitUrl.getRepoCommitUrl(input(0), input(1), input(2), Option(token),None)

  println("Ingestion Service started")

  f.onComplete{
    case Success(value) => println("Success:!!!! "+value)
      Connection.system.shutdown()
    case Failure(value) => println("Ingestion Failed with message: "+value)
      Connection.system.shutdown()
  }
  Await.ready(f,2 hours)

}

