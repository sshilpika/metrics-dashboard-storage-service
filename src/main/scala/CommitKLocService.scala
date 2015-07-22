import java.io.PrintWriter

import _root_.reactivemongo.api.MongoDriver
import akka.actor.ActorSystem
import akka.event.Logging
import akka.io.IO
import akka.pattern._
import akka.util.Timeout
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocumentReader, BSONDocument}
import spray.can.Http
import spray.http.HttpHeaders.RawHeader
import spray.http._
import spray.httpx.RequestBuilding._
import concurrent.ExecutionContext.Implicits._
import concurrent._
import duration._
import java.time._
import spray.json._
import DefaultJsonProtocol._
import scala.util.{Failure, Success}

/**
 * Created by shilpika on 7/17/15.
 */

case class CommitsUrl(url: String, date:String)

object CommitsUrl{
  implicit object PersonReader extends BSONDocumentReader[CommitsUrl]{
    def read(doc: BSONDocument): CommitsUrl = {
      val url = doc.getAs[String]("url").get
      val date = doc.getAs[String]("date").get
      CommitsUrl(url,date)
    }
  }
}


case class CommitsLoc(loc: Int=0, date:String="", rangeLoc:Long=0L)

object CommitsLoc{
  implicit object PersonReader extends BSONDocumentReader[CommitsLoc]{
    def read(doc: BSONDocument): CommitsLoc = {
      val loc = doc.getAs[Int]("loc").get
      val date = doc.getAs[String]("date").get
      val rangeLoc = 0L
      CommitsLoc(loc,date,rangeLoc)
    }
  }
}

case class CommitKLOCInfo(date: String="",loc: Int=0, filename: String="")

object CommitKLOCInfo{
  implicit object PersonReader extends BSONDocumentReader[CommitKLOCInfo]{
    def read(doc: BSONDocument): CommitKLOCInfo = {
      val date = doc.getAs[String]("date").get
      val loc = doc.getAs[Int]("loc").get
      val filename = doc.getAs[String]("filename").get

      CommitKLOCInfo(date,loc,filename)
    }
  }
}
//django/django/master
object ReactiveConnect {
  import reactivemongo.api._
  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))
  val actorsystem = ActorSystem("gitKlocCollection")
}

//get the list of Urls from the DB
object CommitURLService {
  def getMongoUrl(user: String, repo: String, branch: String, accessToken: Option[String]): Future[List[String]] = {

    // get reference of the database
    val db = ReactiveConnect.connection.db(user+"_"+repo+"_"+branch+"_"+"URL")
    val collectionNames = db.collectionNames
    val added = collectionNames.map(p => p.filter(_.contains("COLL")))
    val sorted = added.map(p => p.sorted)

    val totalUrlList = sorted.flatMap(p =>
      //res1 gets data from the DB and saves it in a list of (commitDate, LOC, weekStartDate, weekEndDate)
      Future.sequence(p.map(collName => {
        val collection = db.collection[BSONCollection](collName)
        val collectionsList = collection.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[CommitsUrl].collect[List]()
        val urlList = collectionsList.map(p =>{
          p.map { case (commitUrl) =>
            commitUrl.url
          }})
        urlList
      })).map(_.flatten))
    totalUrlList

  }
  implicit val actorsys = ReactiveConnect.actorsystem

  def storeCommitKlocInfo(user: String, repo: String, branch: String, accessToken: Option[String], urlList : List[String]): Future[List[String]] ={

    implicit  val timeout = Timeout(1 hour)
    val log = Logging(actorsys, getClass)
    val rawHeadersList = accessToken.foldLeft(Nil: List[RawHeader])((list, token) => list:+RawHeader("Authorization", "token "+token))
    val db = ReactiveConnect.connection.db(user+"_"+repo+"_"+branch)
    val result = urlList.map(url => {
      val gitFileCommitList =
        (IO(Http) ? Get(url).withHeaders(rawHeadersList)).mapTo[HttpResponse]

      val tupleList = gitFileCommitList.map(commit => {
          val filesList = commit.entity.data.asString.parseJson.asJsObject.getFields("commit", "files")
          val commitSha = commit.entity.data.asString.parseJson.asJsObject.getFields("sha")(0)
          val date = filesList(0).asJsObject.getFields("committer")(0).asJsObject.getFields("date")(0).compactPrint.replaceAll("\"", "")
          val files = filesList(1).convertTo[List[JsValue]]
          files.map(v => {
            val change = v.asJsObject.getFields("additions", "deletions")
            val loc = change(0).convertTo[Int] - change(1).convertTo[Int]
            val filename = v.asJsObject.getFields("filename")(0).compactPrint.replaceAll("\"","")
            val fileSha = v.asJsObject.getFields("sha")(0)
            // creating collection and calculating the loc to be stored for the current date
            val collectionName = filename.replaceAll("/", "_").replaceAll("\\.", "_")
            val collection = db.collection[BSONCollection](collectionName)
            // cursor might throw exception
            println("dates"+date)
            val selector = BSONDocument("date" -> date)
            val modifier = BSONDocument("$set" -> BSONDocument("date" -> date, "commitSha" -> commitSha.compactPrint,
              "loc" -> loc, "filename" -> filename, "fileSha" -> fileSha.compactPrint))
            collection.update(selector,modifier,multi=true,upsert = true)
            filename
          })

      }) // storage mapping ends for results returned from github
        tupleList

    }) // main url list mapping ends

    val finalFilesList = Future.sequence(result).map(_.flatten)
    finalFilesList
  }

}

object rateLimitCalculator{
  implicit val actorsys = ReactiveConnect.actorsystem
  def calculateRateLimit(accessToken: Option[String]):Future[Int] = {
    implicit val timeout = Timeout(60.seconds)
    val log = Logging(actorsys, getClass)
    val rawHeadersList = accessToken.foldLeft(Nil: List[RawHeader])((list, token) => list:+RawHeader("Authorization", "token "+token))
    val response = (IO(Http) ? Get("https://api.github.com/rate_limit").withHeaders(rawHeadersList)).mapTo[HttpResponse]
    response.map(rateLimit =>{
  rateLimit.headers.filter(x => x.name.equals("X-RateLimit-Remaining"))(0).value.toInt
  })
  }

}

object KlocCollection extends App{
  val lines = scala.io.Source.stdin.getLines
  println("Enter username/reponame/branchname")
  val input = lines.next().split("/")
  println(s"You entered: \nUsername: ${input(0)} \nReponame: ${input(1)} \nBranchname: ${input(2)}\n")
  println("Enter Authentication Token:")
  val token = lines.next()
  println("TOKEN: "+token)
  //implicit val timeout = Timeout(2 days)
  val f = CommitURLService.getMongoUrl(input(0), input(1), input(2), Option(token))

  println("KLOC Service started")

  f.onComplete{
    case Success(urlList) =>
      println("URLLIST:"+urlList.length)
      // get remaining rate limit
      val rate = rateLimitCalculator.calculateRateLimit(Option(token))
      rate.onComplete {
        case Success(rateVal) =>
          //grouping the urlLists to avoid Github rate limit abuse
          val urlListGroups = if (rateVal < 1000){
            val urlListGroup1 = urlList.grouped(rateVal).toList
            Future{urlListGroup1.head:: urlListGroup1.tail.flatten.grouped(1000).toList}
          }else Future{urlList.grouped(1000).toList}
          //Storing the loc info here call is made using groups of url
          val klocInfo = urlListGroups.map(z => z.map(urlLis => {
            println(urlLis.length+"length of mini List")
            val f1 = CommitURLService.storeCommitKlocInfo(input(0), input(1), input(2), Option(token), urlLis)
            f1.onComplete {
              case Success(value) => println("Success in stroing commit info for files"+value.length)
              case Failure(value) => println("Kloc storage failed with message: ")
                value.printStackTrace()
                ReactiveConnect.actorsystem.shutdown()
            }
            Await.result(f1,1 hour)
            println("BEFORE NEXT CALL")
            if(urlList.length >5000 && rateVal >1000)
            Thread.sleep(15*60*1000)
            println("NEXT CALL")
          }))
          klocInfo.onComplete{
            case Success(_) =>
              val f3 = KlocGenerator.sortLoc(input(0), input(1), input(2), Option(token))
              f3.onComplete{
                case Success(v)=> println("Loc done!")
                case Failure(v) => println("Loc and range calculations failed")
                  v.printStackTrace()
              }
            case Failure(_) => println("Failure during Kloc modification!")
          }
        case Failure(rateError) => println("rate retrieval failed:"+rateError)
          rateError.printStackTrace()
      }
    case Failure(value) => println("Url collection failed with message: "+value)
      ReactiveConnect.actorsystem.shutdown()
  }

}

//Update Range*LOC info in the KLOC document

object KlocGenerator {

  def sortLoc(user: String, repo: String, branch: String, accessToken: Option[String]): Future[List[CommitsLoc]] ={
    // get reference of the database
    val db = ReactiveConnect.connection.db(user+"_"+repo+"_"+branch)
    val allCollectionNames = db.collectionNames
    val collectionNames = allCollectionNames.map(p => p.filter(!_.equals("system.indexes")))
    val totalUrlList = collectionNames.flatMap(p =>
      //res1 gets data from the DB and saves it in a list of (commitDate, LOC, weekStartDate, weekEndDate)
      Future.sequence(p.map(collName => {
        val collection = db.collection[BSONCollection](collName)
        val collectionsListSorted = collection.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[CommitsLoc].collect[List]()
        collectionsListSorted.map(p =>{
          //calculating the incremental Loc per file
        val newCommitLocLis = p.scanLeft(CommitsLoc(0,"",0L)){(a,c) => CommitsLoc(a.loc+c.loc,c.date,c.rangeLoc)}.tail
          println(newCommitLocLis)
         val updatedRes =  newCommitLocLis.map(commitLoc =>{
            val selector = BSONDocument("date" -> commitLoc.date)
            println("COMMIT LOC:"+commitLoc.loc)
            val modifier = BSONDocument(
              "$set" -> BSONDocument(
                "loc" -> commitLoc.loc))
            collection.update(selector, modifier)
          })
          val rangeLocF = Future.sequence(updatedRes).flatMap(x =>{
              val l2 = newCommitLocLis.zip(newCommitLocLis.tail:+newCommitLocLis(newCommitLocLis.length-1))
              Future.sequence(l2.map(x => {
                val ldt = ZonedDateTime.ofInstant(Instant.parse(x._2.date),ZoneId.of("UTC"))
                val range = java.time.Duration.between(Instant.parse(x._1.date),Instant.parse(x._2.date)).toMillis/1000
                val selector = BSONDocument("date" -> x._1.date)
                println(range+"range!!!")
                val modifier = BSONDocument(
                  "$set" -> BSONDocument(
                    "rangeLoc" -> range))
                collection.update(selector, modifier)
              }))})
          Await.result(rangeLocF,1 hour)
          newCommitLocLis
        })

      })).map(_.flatten))
    totalUrlList
  }
}
