package edu.luc.cs.metrics.ingestion.service

import java.time._
import java.time.temporal.TemporalAdjusters
import akka.event.Logging
import com.mongodb.casbah.Imports._
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


case class CommitsLoc(loc: Int=0, date:String="", filename: String="", rangeLoc:Long=0L,sortedFlag:Boolean)

object CommitsLoc{
  implicit object PersonReader extends BSONDocumentReader[CommitsLoc]{
    def read(doc: BSONDocument): CommitsLoc = {
      val loc = doc.getAs[Int]("loc").get
      val date = doc.getAs[String]("date").get
      val rangeLoc = 0L
      val filename = doc.getAs[String]("filename").get
      val sorted = doc.getAs[Boolean]("sorted").get
      CommitsLoc(loc,date,filename, rangeLoc,sorted)
    }
  }
}


//get the list of Urls from the DB
object CommitKLocService extends Ingestion with CommitKlocIngestion{

  def getMongoUrl(dbName: String, accessToken: Option[String]): Future[List[String]] = {

    val db = reactiveMongoDb(dbName+"_"+"URL")
    val sortedUrls = db.collectionNames map(p => p.filter(_.contains("COLL"))) map(_.sorted)
    sortedUrls.flatMap(p =>
      Future.sequence(p.map(collName => {
        val collection = db.collection[BSONCollection](collName)
        val collectionsList = collection.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[CommitsUrl].collect[List]()
        collectionsList.map(p1 =>{
          p1.map { case (commitUrl) =>
            commitUrl.url
          }})
      })).map(_.flatten))
  }

  def getSelectedMongoUrl(dbName: String, accessToken: Option[String], collectionNames: List[String]): Future[List[String]] = {

    val db = reactiveMongoDb(dbName+"_"+"URL")
    val sortedUrls = collectionNames
    Future.sequence(sortedUrls.map(collName => {
        val collection = db.collection[BSONCollection](collName)
        val collectionsList = collection.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[CommitsUrl].collect[List]()
        collectionsList.map(p1 =>{
          p1.map { case (commitUrl) =>
            commitUrl.url
          }})
      })).map(_.flatten)
  }


  def storeCommitKlocInfo(user: String, repo: String, branch: String, accessToken: Option[String], urlList : List[String]): Future[List[String]] ={

    val log = Logging(actorsys, getClass)
    val db = /*mongoClientCasbah1(user+"_"+repo+"_"+branch)*/ reactiveMongoDb(user+"_"+repo+"_"+branch)
    val result = urlList.map(url => {
      //implicit val timeout = timeout(1 hour)
      val gitFileCommitList = getHttpResponse(url,rawHeaderList(accessToken),1 hour)

      val resultF = gitFileCommitList.map(commit => {
        //log.info("GOT THIS FROM GITHUB --->"+commit.entity.data.asString)
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
            val fname = filename.replaceAll("/", "_").replaceAll("\\.", "_")
            val flen = fname.length
            val collectionName = if(flen > 55) fname.substring(flen-55)
            else
              fname
            log.info(date+" "+commitSha.compactPrint+" "+loc+" "+filename+" "+fileSha.compactPrint)
            /*val collection = db.getCollection(collectionName)
            collection.update(MongoDBObject("date" -> date),$set("date" -> date, "commitSha" -> commitSha.compactPrint,
              "loc" -> loc, "filename" -> filename, "fileSha" -> fileSha.compactPrint, "sorted"-> false),true,true)*/
            val collection = db.collection[BSONCollection](collectionName)
            // cursor might throw exception
            val selector = BSONDocument("date" -> date)
            val modifier = BSONDocument("$set" -> BSONDocument("date" -> date, "commitSha" -> commitSha.compactPrint,
              "loc" -> loc, "filename" -> filename, "fileSha" -> fileSha.compactPrint, "sorted"-> false))
            collection.update(selector,modifier,multi=true,upsert = true)
            /*collection.update(MongoDBObject("date" -> date),$set("date" -> date, "commitSha" -> commitSha.compactPrint,
              "loc" -> loc, "filename" -> filename, "fileSha" -> fileSha.compactPrint),true,true)*/
            filename
          })

      }) // storage mapping ends for results returned from github
      Await.result(gitFileCommitList,20 minutes)
      resultF
    }) // main url list mapping ends
    Future.sequence(result).map(_.flatten)
  }

  //Update Range*LOC info in the KLOC document
  def sortLoc(user: String, repo: String, branch: String): List[CommitsLoc] ={
    // get reference of the database
    import com.mongodb.casbah.Imports._
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient(user+"_"+repo+"_"+branch)
    log.info("Unfiltered list of collections"+db.collectionNames)
    val colls = db.collectionNames filter(!_.equals("system.indexes")) filter(!_.contains("system_indexes_defect_density"))
    log.info("This is collections for sort loc function :"+colls)
    colls.toList flatMap(collName => {
        val col = db(collName)
        val newCommitLocLis = col.find().sort(MongoDBObject("date" -> 1)).toList.scanLeft(CommitsLoc(0,"","",0L,false)){(a,x) =>

          CommitsLoc(a.loc+x.getAs[Int]("loc").get,x.getAs[String]("date").get,x.getAs[String]("filename").get,x.getAs[Long]("rangeLoc").getOrElse(0),x.getAs[Boolean]("sorted").get)}.tail
        val updatedRes =  newCommitLocLis.map(commitLoc =>{
          val selector = MongoDBObject("date" -> commitLoc.date)
          val ins: Instant = Instant.parse(commitLoc.date)
          val zdt =  ZonedDateTime.ofInstant(ins, ZoneId.of("UTC"))
          val modifier = $set("loc" -> commitLoc.loc,"sorted"->true)
          col.update(selector, modifier,true,true)
        })
        val l2 = newCommitLocLis.zip(newCommitLocLis.tail:+newCommitLocLis(newCommitLocLis.length-1))
        l2.map(x => {
          val ldt = ZonedDateTime.ofInstant(Instant.parse(x._2.date),ZoneId.of("UTC"))
          val range = java.time.Duration.between(Instant.parse(x._1.date),Instant.parse(x._2.date)).toMillis/1000
          val selector = MongoDBObject("date" -> x._1.date)
          val modifier = $set("rangeLoc" -> range)
          col.update(selector, modifier,true,true)
        })
        newCommitLocLis
      })

  }

  def sorSelectedtLoc(user: String, repo: String, branch: String): List[CommitsLoc] ={
    // get reference of the database
    import com.mongodb.casbah.Imports._
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient(user+"_"+repo+"_"+branch)
    val colls = db.collectionNames filter(!_.equals("system.indexes")) filter(!_.contains("system_indexes_defect_density"))
    colls.toList flatMap(collName => {
      val col = db(collName)
     // val commitLocList = col.find().sort(MongoDBObject("date" -> 1)).toList.
      val newCommitLocLis = col.find().sort(MongoDBObject("date" -> 1)).toList.scanLeft(CommitsLoc(0,"","",0L,true)){(a,x) =>
        if(!x.getAs[Boolean]("sorted").get)
          CommitsLoc(a.loc+x.getAs[Int]("loc").get,x.getAs[String]("date").get,x.getAs[String]("filename").get,x.getAs[Long]("rangeLoc").getOrElse(0),x.getAs[Boolean]("sorted").get)
        else
          CommitsLoc(x.getAs[Int]("loc").get,x.getAs[String]("date").get,x.getAs[String]("filename").get,x.getAs[Long]("rangeLoc").getOrElse(0),x.getAs[Boolean]("sorted").get)
      }.tail


      val updatedRes =  newCommitLocLis.map(commitLoc =>{
        if(!commitLoc.sortedFlag) {
          val selector = MongoDBObject("date" -> commitLoc.date)
          val ins: Instant = Instant.parse(commitLoc.date)
          val zdt = ZonedDateTime.ofInstant(ins, ZoneId.of("UTC"))
          val modifier = $set("loc" -> commitLoc.loc, "sorted" -> true)
          col.update(selector, modifier, true, true)
        }
      })
      val l2 = newCommitLocLis.zip(newCommitLocLis.tail:+newCommitLocLis(newCommitLocLis.length-1))
      l2.map(x => {
        if(!x._2.sortedFlag) {
          val ldt = ZonedDateTime.ofInstant(Instant.parse(x._2.date), ZoneId.of("UTC"))
          val range = java.time.Duration.between(Instant.parse(x._1.date), Instant.parse(x._2.date)).toMillis / 1000
          val selector = MongoDBObject("date" -> x._1.date)
          val modifier = $set("rangeLoc" -> range)
          col.update(selector, modifier, true, true)
        }
      })
      newCommitLocLis
    })

  }

}


