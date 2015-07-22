import java.time.{ZoneId, LocalDateTime, Instant}
import util._
import akka.io.IO
import com.mongodb.casbah.Imports._
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http.HttpResponse
import spray.httpx.RequestBuilding._
import spray.json._
import DefaultJsonProtocol._
import concurrent._
import ExecutionContext.Implicits._
import duration._
import akka.event.Logging
import akka.util.Timeout
import akka.pattern.ask
/**
 * Created by shilpika on 7/19/15.
 */


object IssuesStorage{

  def storeGithubIssues(user: String, repo: String, branch:String, accessToken: Option[String], page: Option[String]): Future[String] ={
    implicit val system = Connection.system
    val log = Logging(system, getClass)
    implicit val timeout = Timeout(60.seconds)
    val rawHeadersList = accessToken.foldLeft(Nil: List[RawHeader])((list, token) => list:+RawHeader("Authorization", "token "+token))
    val url = if(page.isDefined){"https://api.github.com/repos/"+user+"/"+repo+"/issues?"+page.get} else "https://api.github.com/repos/"+user+"/"+repo+"/issues"

    val gitIssueList =
      (IO(Http) ? Get(url).withHeaders(rawHeadersList)).mapTo[HttpResponse]

    val nextUrlForCurrentWeek = gitIssueList.map(issueList => {
      println("This is a list of headers:")
      val link = issueList.headers.filter(x => x.name.equals("Link"))
      //val link = link1.(0)
      issueList.headers.map(x => println(x.name+ "!!!!!!!!!!!!!!!!" +x.value))
      println(link)

      val rateTime =  issueList.headers.filter(x => x.name.equals("X-RateLimit-Reset"))(0).value
      val rateRemaining = issueList.headers.filter(x => x.name.equals("X-RateLimit-Remaining"))(0).value
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
      })
      else
        None

      //extracting urls from the commit info obtained and storing urlList in the DB
      val mongoClient = Connection.mongoClient
      val db = mongoClient(user+"_"+repo+"_"+"Issues")
      issueList.entity.data.asString.parseJson.convertTo[List[JsValue]].map(commits => {

        val createdAt = commits.asJsObject.getFields("created_at")(0).compactPrint.replaceAll("\"", "")
        val ldt = LocalDateTime.ofInstant(Instant.parse(createdAt),ZoneId.of("UTC"))
        val start_date = Instant.parse(createdAt).minus(java.time.Duration.ofDays(ldt.getDayOfWeek.getValue-1))
        val since = LocalDateTime.ofInstant(start_date,ZoneId.of("UTC"))
        val coll = db("COLL"+ldt.getYear+"ISSUE"+since.getDayOfYear)
        val url = commits.asJsObject.getFields("url")(0).compactPrint.replaceAll("\"","")
        val number = commits.asJsObject.getFields("number")(0).compactPrint.replaceAll("\"","")
        val state = commits.asJsObject.getFields("state")(0).compactPrint.replaceAll("\"","")
        val id = commits.asJsObject.getFields("id")(0).compactPrint.replaceAll("\"","")
        val title = commits.asJsObject.getFields("title")(0).compactPrint.replaceAll("\"","")
        val body = commits.asJsObject.getFields("body")(0).compactPrint.replaceAll("\"","")
        coll.insert(MongoDBObject("id" -> id, "url"-> url, "date" -> createdAt, "number"-> number, "state" -> state, "title" -> title, "body" -> body))

      })

      //the url for next page of commits
      println("nextUrlForCurrentWeek: "+nextUrlForCurrentWeek)
      nextUrlForCurrentWeek
    })

    nextUrlForCurrentWeek.flatMap(nextUrl => nextUrl.map(page =>{
      storeGithubIssues(user,repo,branch,accessToken,Option(page))
    } ).getOrElse(Future("")))

  }
}

object IssueStorageService extends App{

  val lines = scala.io.Source.stdin.getLines
  println("Enter username/reponame/branchname")
  val input = lines.next().split("/")

  println(s"You entered: \nUsername: ${input(0)} \nReponame: ${input(1)} \nBranchname: ${input(2)}\n")

  println("Enter Authentication Token:")
  val token = lines.next()
  println("TOKEN: "+token)

  val f = IssuesStorage.storeGithubIssues(input(0), input(1), input(2), Option(token),None)

  f.onComplete{
    case Success(value) => println("Success: "+value)
      Connection.system.shutdown()
    case Failure(value) => println("Ingestion Failed with message: "+value)
      value.printStackTrace()
      Connection.system.shutdown()
  }
  println("DONE!!!")

}


