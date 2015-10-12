package edu.luc.cs.metrics.ingestion.service

import java.io.PrintWriter
import java.time.temporal.TemporalAdjusters
import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import akka.actor.Actor
import akka.util.Timeout
import com.mongodb.casbah.Imports._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONDocumentReader}
import spray.json._
import spray.routing.HttpService
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Await, Future}
import scala.util._
import concurrent.duration._

/**
 * Created by sshilpika on 6/30/15.
 */

case class IssueState(Open:Int, Close:Int)
case class LocIssue(startDate: String, endDate:String,kloc:Double, issues: IssueState)

object JProtocol extends DefaultJsonProtocol{
  implicit val IssueInfoResult:RootJsonFormat[IssueState] = jsonFormat(IssueState,"open","closed")
  implicit val klocFormat:RootJsonFormat[LocIssue] = jsonFormat(LocIssue,"start_date","end_date","kloc","issues")
}

case class JsonPResult(commitInfo: JsValue)

object JsonPProtocol{
  import spray.json.DefaultJsonProtocol._
  implicit val gitResult = jsonFormat(JsonPResult,"defectDensity")
}

case class CommitInfo(date: String,loc: Int, filename: String, rangeLoc: Long)

object CommitInfo{
  implicit object PersonReader extends BSONDocumentReader[CommitInfo]{
    def read(doc: BSONDocument): CommitInfo = {
      val date = doc.getAs[String]("date").get
      val loc = doc.getAs[Int]("loc").get
      val filename = doc.getAs[String]("filename").get
      val rangeLoc = doc.getAs[Long]("rangeLoc").get
      CommitInfo(date,loc,filename,rangeLoc)
    }
  }
}

case class IssueInfo(date: String,state: String/*, created_at: String, closed_at: String*/)

object IssueInfo{
  implicit object PersonReader extends BSONDocumentReader[IssueInfo]{
    def read(doc: BSONDocument): IssueInfo = {
      val date = doc.getAs[String]("date").get
      val state = doc.getAs[String]("state").get
      /*val created_at = doc.getAs[String]("created_at").get
      val closed_at = doc.getAs[String]("closed_at").get*/
      IssueInfo(date,state/*,created_at, closed_at*/)
    }
  }
}

object CommitDensityService extends ingestionStrategy{
  //import com.mongodb.casbah.Imports._
  //val mongoClient = mongoCasbah("localhost", 27017)

  def getIssues(user: String, repo: String, branch:String, groupBy: String, klocList:Map[Instant,(Instant,Double,(Int,Int))]): Future[JsValue] ={
    val connection = mongoConnection
    //gets a reference of the database for commits
    println("Getting issues now!!!")
    val db = connection.db(user+"_"+repo+"_Issues")
    val collection = db.collectionNames
    val timeout = Timeout(1 hour)
    val finalRes = collection.map(_.filter(!_.contains("system.indexes"))) flatMap(p =>{
      println(p.length+"No of collections")
      val res = Future.sequence(p.map(collName => {
        val coll = db.collection[BSONCollection](collName)
        val issuesListF = coll.find(BSONDocument()).sort(BSONDocument("date" -> 1)).cursor[IssueInfo].collect[List]()

        issuesListF.map(_.map(issueDoc => {
          val inst = Instant.parse(issueDoc.date)
          val ldt = ZonedDateTime.ofInstant(inst, ZoneId.of("UTC"))
          val startDate = if (groupBy.equals("week")) //weekly
          //firstDateOfCommit.minus(Duration.ofDays(ldt.getDayOfWeek.getValue - 1))
            inst.minus(Duration.ofDays(ldt.getDayOfWeek.getValue - 1))
          else {//monthly
            inst.minus(Duration.ofDays(ldt.getDayOfMonth - 1))
            val lddt = ZonedDateTime.ofInstant(inst, ZoneId.of("UTC"))//.withHour(0).withMinute(0).withSecond(0)
            lddt.`with`(TemporalAdjusters.firstDayOfMonth()).toInstant
          }

          val startDate2 =
            if (groupBy.equals("week")) {
              //weekly
              inst.minus(Duration.ofDays(ldt.getDayOfWeek.getValue - 1))
            } else {
              //monthly
              ldt.`with`(TemporalAdjusters.firstDayOfMonth()).toInstant //inst.minus(Duration.ofDays(ldt.getDayOfMonth-1))
            }

          val startDate1 = ZonedDateTime.ofInstant(startDate2, ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0).toInstant
          //println(startDate1+" THis is start date!!!!!")
          if(klocList.keys.exists(_ == startDate1)) {
            val startD = klocList.keys.filter(x => {
              /*println(x.toString+" This is x string "+startDate1);*/ x.toString.contains(startDate1.toString.substring(0, 11))
            })
            //println("STARDDDDD "+startD)
            val startCheck = startD.head
            val mapValue = klocList get startCheck get //OrElse(0)
            val openState = if (issueDoc.state.equals("open")) mapValue._3._1 + 1 else mapValue._3._1
            val closeState = if (issueDoc.state.equals("closed")) mapValue._3._2 + 1 else mapValue._3._2
            klocList + (startCheck ->(mapValue._1, mapValue._2, (openState, closeState))) //updates old value with new value
          }else{
            klocList //+ (startCheck ->(mapValue._1, mapValue._2, (openState, closeState)))
          }
        }))

      })).map(_.flatten)
      res.map(p => {
        val x = p/*.map(_.toIterable)*/.toIterable.flatten.groupBy(y => y._1)/*foldLeft(Nil: Iterable[(Instant,(Instant,Double,(Int,Int)))]){(i,res) => {
          val b = i.toIterator.duplicate
          val Iterelem = b._1.filter(_._1 == res._1)
          val elem = Iterelem.toList
          //println("ELEM "+elem)
          if(!elem.isEmpty){
            val res1 = (res._1,(res._2._1,res._2._2,(res._2._3._1+elem(0)._2._3._1,res._2._3._2+elem(0)._2._3._2)))
            (b._2 ++ List(res1).toIterator).toIterable
          }else{
            (b._2 ++ List(res).toIterable).toIterable
          }

        }}*///groupBy(y => y._1)
        x.toIterable.map(y => (y._1,y._2.foldLeft((Instant.now(),0.0D,(0,0)):(Instant,Double,(Int,Int))){(acc,z) => (z._2._1,z._2._2,(z._2._3._1+acc._3._1,z._2._3._2+acc._3._2))}))
      })
    })
    val jsonifyRes = finalRes.map(_.foldLeft(Nil: Iterable[LocIssue]){(x,y) => {
      val totalRange = (Duration.between(y._1,y._2._1).toMillis).toDouble/1000
      x ++ List(LocIssue(y._1.toString, y._2._1.toString,((y._2._2)/1000)/totalRange,IssueState(y._2._3._1,y._2._3._2))).toIterable
    }})
    //println("jsonify")
    jsonifyRes.map(x => {/*println("jsonify result"+x);*/import JProtocol._;/*x.sortBy(_.startDate).toJson*/
      println("jsonify")
    x.toJson
    })
  }



  def getKloc(dbName: String, groupBy:String):Map[Instant,(Instant,Double,(Int,Int))] ={

    val db = mongoCasbah(dbName)
    val collections = db.collectionNames()
    println("Total No of collections"+collections.size)
    val filteredCol = collections.filter(!_.equals("system.indexes")).filter(!_.contains("system_indexes_defect_density"))
    println("Filtered collections "+filteredCol.size)

    // commits count for files
    val commitCount = filteredCol.flatMap(coll =>{
      val eachColl = db(coll)
      val documentLis = eachColl.find().sort(MongoDBObject("date"-> 1)).toList map(y => {
        CommitInfo(y.getAs[String]("date") get,y.getAs[Int]("loc") get,y.getAs[String]("filename") get,y.getAs[Long]("rangeLoc").getOrElse(0L))})
      // documentLis is the list of documents in the collection. NOTE that these documents are all sorted in ascending order !!!!
      //transform sorted document list into list of sorted dateRange for the file(document in the DB)
      val startDateForFile = documentLis(0).date
      val inststartDateForFile = Instant.parse(startDateForFile)
      val ldt = ZonedDateTime.ofInstant(inststartDateForFile, ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0)
      val startDate_groupBy_file =
        if (groupBy.equals("week")) {
          //weekly
          val temp = inststartDateForFile.minus(java.time.Duration.ofDays(ldt.getDayOfWeek.getValue - 1))
          ZonedDateTime.ofInstant(temp, ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0).toInstant
        } else {
          //monthly
          ldt.`with`(TemporalAdjusters.firstDayOfMonth()).toInstant
        }
      //generate empty list to hold all values for the files commit history (in other words, generate an empty list for each collection to hold values of documents
      val now = Instant.now()
      val dateRangeLength = if (groupBy.equals("week")) {
        //weekly
        val daysBtw = java.time.Duration.between(startDate_groupBy_file, now).toDays
        if (daysBtw % 7 != 0)
          (daysBtw / 7).toInt + 1
        else
          (daysBtw / 7).toInt
      } else {
        //monthly
        val zonedStart = ZonedDateTime.ofInstant(startDate_groupBy_file, ZoneId.of("UTC"))
        val zonedNow = ZonedDateTime.ofInstant(now, ZoneId.of("UTC"))
        import java.time.temporal.ChronoUnit
        ChronoUnit.MONTHS.between(zonedStart,zonedNow).toInt +1
      }
      //empty list to iterate and fill with valid values, currently contains dummy values
      val l1 = List.fill(dateRangeLength)(("SD", "ED", 0.0D, (0, 0)))

      val dateRangeList = l1.scanLeft((startDate_groupBy_file, startDate_groupBy_file, 0.0D, (0, 0)))((a, x) => {
        if (groupBy.equals("week")) {
          val startOfWeek = ZonedDateTime.ofInstant(a._2, ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0)
          val endOfWeek = ZonedDateTime.ofInstant(a._2.plus(java.time.Duration.ofDays(7)), ZoneId.of("UTC")).withHour(23).withMinute(59).withSecond(59)
          (startOfWeek.toInstant, endOfWeek.toInstant, x._3, x._4)
        } else {
          val localDT = ZonedDateTime.ofInstant(a._2, ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0)
          val firstDayOfMonth =
            if (a._2 == startDate_groupBy_file)
              localDT.`with`(TemporalAdjusters.firstDayOfMonth())
            else
              localDT.`with`(TemporalAdjusters.firstDayOfNextMonth())
          val lastDayOfMonth = firstDayOfMonth.`with`(TemporalAdjusters.lastDayOfMonth()).withHour(23).withMinute(59).withSecond(59)
          (firstDayOfMonth.toInstant(), lastDayOfMonth.toInstant(), x._3, x._4)
        }
      }).tail

      var previousLoc = 0
      val lastDateOfCommit = Instant.parse(documentLis(documentLis.length - 1).date)
      val finalres = dateRangeList.map(x => {
        val commitInfoForRange1 = documentLis.filter { dbVals => {
          val ins = Instant.parse(dbVals.date); ins.isAfter(x._1) && ins.isBefore(x._2)
        }
        }.sortBy(_.date)
        val res = if (!commitInfoForRange1.isEmpty) {
          // if commitsInfo is present within the range
          val commitInfoForRange = CommitInfo(x._1.toString, previousLoc, "", java.time.Duration.between(x._1, Instant.parse(commitInfoForRange1(0).date)).toMillis / 1000) +:
            commitInfoForRange1
          val commitInfoForRange2 = commitInfoForRange.take(commitInfoForRange.length - 1) :+ CommitInfo(commitInfoForRange.last.date, commitInfoForRange.last.loc,
            commitInfoForRange.last.filename, java.time.Duration.between(Instant.parse(commitInfoForRange.last.date), x._2).toMillis / 1000)
          previousLoc = commitInfoForRange(commitInfoForRange.length - 1).loc
          val rangeCalulated = commitInfoForRange2.foldLeft(0L: Long) { (a, commitInf) => a + (commitInf.rangeLoc * commitInf.loc)}
          (x._1, x._2, rangeCalulated.toDouble, x._4)
        } else if (x._1.isAfter(lastDateOfCommit)) {
          // if the range falls after the last commit for the file
          val rangeLoc = (documentLis(documentLis.length - 1).loc) * ((java.time.Duration.between(x._1, x._2).toMillis) toDouble) / 1000
          (x._1, x._2, rangeLoc, x._4)
        } else if (x._1.isAfter(inststartDateForFile) && x._1.isBefore(lastDateOfCommit)) {
          // if the range falls inside the commits lifetime of the file but is empty
          val commLis = documentLis.filter { dbVals => {
            val ins = Instant.parse(dbVals.date); ins.isBefore(x._1)
          }
          }
          val rangeLoc = (commLis.sortBy(_.date).reverse(0).loc) * ((java.time.Duration.between(x._1, x._2).toMillis) toDouble) / 1000
          (x._1, x._2, rangeLoc, x._4)
        } else {
          (x._1, x._2, 0.0D, x._4)
        }
        res
      })
      finalres

    }).toList
    val result = commitCount.groupBy(_._1) map(y => (y._1,{
      //println("Flatten")
      val rangeLoc = y._2.foldLeft(0D)((acc,z) => acc+z._3)
      (y._2(0)._2,rangeLoc,y._2(0)._4)
    }))
    //result
    println("Total Commits = "+result)
    //mongoConnectionClose
    result
  }

  def dataForDefectDensity(user: String, repo: String, branch:String, groupBy: String): Future[JsValue] ={

    val kloc = getKloc(user+"_"+repo+"_"+branch, groupBy)
    println("This is kloc"+kloc)
    val writer = new PrintWriter(new java.io.File("store.txt"))
    val k = kloc.toList.sortBy(_._1)
    writer.write(k.toString())
    writer.close()
    val defectDensityResult = getIssues(user,repo,branch, groupBy, kloc)
    defectDensityResult.onComplete{
      case Success(v) =>
        /*val connection = mongoConnection
        val db = connection.db(user + "_" + repo + "_" + branch)
        val collection = db.collection[BSONCollection]("system_indexes_defect_density")
        val document = BSONDocument("DefectDensity" -> v.compactPrint)
        val f = collection.insert(document)
        f.onComplete{
          case Success(v) => println("Collection saved")
          case Failure(v) => v.printStackTrace()
        }
        Await.result(f, 1 hour)*/
        dbStore(DefectDensity(v, mongoCasbah(user + "_" + repo + "_" + branch),groupBy))

        println("KLOC sorted and defect density stored!")
        actorsys.shutdown()
      case Failure(v) => println("Loc and range calculations failed")
        v.printStackTrace()
        actorsys.shutdown()
    }
    Await.result(defectDensityResult, 1 hour)

    defectDensityResult

  }



}



