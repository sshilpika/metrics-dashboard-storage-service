package edu.luc.cs.metrics.ingestion.service

import java.time._

import akka.event.Logging
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONDocumentReader}
import spray.json._
import spray.json.DefaultJsonProtocol._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent._
import scala.concurrent.duration._

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


case class CommitsLoc(loc: Int=0, date:String="", filename: String="", rangeLoc:Long=0L)

object CommitsLoc{
  implicit object PersonReader extends BSONDocumentReader[CommitsLoc]{
    def read(doc: BSONDocument): CommitsLoc = {
      val loc = doc.getAs[Int]("loc").get
      val date = doc.getAs[String]("date").get
      val rangeLoc = 0L
      val filename = doc.getAs[String]("filename").get
      CommitsLoc(loc,date,filename, rangeLoc)
    }
  }
}


//get the list of Urls from the DB
object CommitKLocService extends Ingestion with CommitKlocIngestion{

  def getMongoUrl(user: String, repo: String, branch: String, accessToken: Option[String]): Future[List[String]] = {

    val db = reactiveMongoDb(user+"_"+repo+"_"+branch+"_"+"URL")
    val sortedUrls = db.collectionNames map(p => p.filter(_.contains("COLL"))) map(_.sorted)
    sortedUrls.flatMap(p =>
      Future.sequence(p.map(collName => {
        val collection = db.collection[BSONCollection](collName)
        val collectionsList = collection.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[CommitsUrl].collect[List]()
        collectionsList.map(p =>{
          p.map { case (commitUrl) =>
            commitUrl.url
          }})
      })).map(_.flatten))
  }


  def storeCommitKlocInfo(user: String, repo: String, branch: String, accessToken: Option[String], urlList : List[String]): Future[List[String]] ={

    val log = Logging(actorsys, getClass)
    val db = reactiveMongoDb(user+"_"+repo+"_"+branch)
    val result = urlList.map(url => {
      //implicit val timeout = timeout(1 hour)
      val gitFileCommitList = getHttpResponse(url,rawHeaderList(accessToken),1 hour)

      gitFileCommitList.map(commit => {
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
            val selector = BSONDocument("date" -> date)
            val modifier = BSONDocument("$set" -> BSONDocument("date" -> date, "commitSha" -> commitSha.compactPrint,
              "loc" -> loc, "filename" -> filename, "fileSha" -> fileSha.compactPrint))
            collection.update(selector,modifier,multi=true,upsert = true)
            filename
          })

      }) // storage mapping ends for results returned from github
    }) // main url list mapping ends
    Future.sequence(result).map(_.flatten)
  }

  //Update Range*LOC info in the KLOC document
  def sortLoc(user: String, repo: String, branch: String, accessToken: Option[String]): Future[List[CommitsLoc]] ={
    // get reference of the database
    val db = reactiveMongoDb(user+"_"+repo+"_"+branch)
    db.collectionNames map(_.filter(!_.equals("system.indexes"))) flatMap(p =>
      //res1 gets data from the DB and saves it in a list of (commitDate, LOC, weekStartDate, weekEndDate)
      Future.sequence(p.map(collName => {
        val collection = db.collection[BSONCollection](collName)
        val collectionsListSorted = collection.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[CommitsLoc].collect[List]()
        collectionsListSorted.map(p =>{
          //calculating the incremental Loc per file
          val newCommitLocLis = p.scanLeft(CommitsLoc(0,"","",0L)){(a,c) => CommitsLoc(a.loc+c.loc,c.date,c.filename,c.rangeLoc)}.tail
          //println(newCommitLocLis)
          val updatedRes =  newCommitLocLis.map(commitLoc =>{
            val selector = BSONDocument("date" -> commitLoc.date)
            println("COMMIT LOC:"+commitLoc.loc)
            val modifier = BSONDocument(
              "$set" -> BSONDocument(
                "loc" -> commitLoc.loc))
            collection.update(selector, modifier)
          })
          val rangeStoreFuture = Future.sequence(updatedRes).flatMap(x =>{
            val l2 = newCommitLocLis.zip(newCommitLocLis.tail:+newCommitLocLis(newCommitLocLis.length-1))
            Future.sequence(l2.map(x => {
              val ldt = ZonedDateTime.ofInstant(Instant.parse(x._2.date),ZoneId.of("UTC"))
              val range = java.time.Duration.between(Instant.parse(x._1.date),Instant.parse(x._2.date)).toMillis/1000
              val selector = BSONDocument("date" -> x._1.date)
              val modifier = BSONDocument(
                "$set" -> BSONDocument(
                  "rangeLoc" -> range))
              collection.update(selector, modifier)
            }))})
          Await.result(rangeStoreFuture,1 hour)
          newCommitLocLis
        })

      })).map(_.flatten))
  }

}


