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

case class IssueState(Open:Int, Close:Int, openCumulative: Int, closedCumulative:Int)
case class LocIssue(startDate: String, endDate:String,kloc:Double, issues: IssueState)
case class IssueSpoilage(startDate: String, endDate:String,issueSpoilageResult:Double)

object SpoilageJProtocol extends DefaultJsonProtocol{
  implicit val spoilageFormat:RootJsonFormat[IssueSpoilage] = jsonFormat(IssueSpoilage,"start_date","end_date","issue_spoilage")
}

object JProtocol extends DefaultJsonProtocol{
  implicit val IssueInfoResult:RootJsonFormat[IssueState] = jsonFormat(IssueState,"open","closed","openCumulative","closedCumulative")
  implicit val klocFormat:RootJsonFormat[LocIssue] = jsonFormat(LocIssue,"start_date","end_date","kloc","issues")
}



sealed trait MetricsInfo
case class CommitInfo(date: String,loc: Int, filename: String, rangeLoc: Long) extends MetricsInfo

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

case class IssueInfo(date: String,state: String, created_at: String, closed_at: String) extends MetricsInfo

object IssueInfo{
  implicit object PersonReader extends BSONDocumentReader[IssueInfo]{
    def read(doc: BSONDocument): IssueInfo = {
      val date = doc.getAs[String]("date").get
      val state = doc.getAs[String]("state").get
      val created_at = doc.getAs[String]("created_at").get
      val closed_at = doc.getAs[String]("closed_at").get
      IssueInfo(date,state,created_at, closed_at)
    }
  }
}

object CommitDensityService extends ingestionStrategy{
  //import com.mongodb.casbah.Imports._
  //val mongoClient = mongoCasbah("localhost", 27017)

  def getIssues(user: String, repo: String, branch:String, groupBy: String, klocList:Map[Instant,(Instant,Double,(Int,Int))]): JsValue ={

    val connection = mongoConnection
    //gets a reference of the database for commits
    println("Getting issues now!!!")
    /*val db = connection.db(user+"_"+repo+"_Issues")
    val collection = db.collectionNames*/
    val db = mongoCasbah(user+"_"+repo+"_Issues")
    val collection = db.collectionNames()
    //val timeout = Timeout(1 hour)
    val finalRes = collection.filter(!_.contains("system.indexes")).toList.foldLeft(klocList){(klocList2,coll) => {

      val eachColl = db(coll)
      //println("EACH COLL:"+eachColl)
      val documentLis = eachColl.find().sort(MongoDBObject("date" -> 1)).toList map (y => {
        IssueInfo(y.getAs[String]("date") get, y.getAs[String]("state") get, y.getAs[String]("created_at") get,y.getAs[String]("closed_at") get)
      })

      klocList2 ++ documentLis.foldLeft(klocList2){(klocList1,issueDoc) => {
        val startCheckOpen: Option[Instant] = getIssuesKlocDate(groupBy, klocList1, issueDoc.created_at)

        if(startCheckOpen.isDefined){
        val mapValueOpen = klocList1 get startCheckOpen.get get
        val klocListTemp = klocList1 + (startCheckOpen.get ->(mapValueOpen._1, mapValueOpen._2, (mapValueOpen._3._1 + 1, mapValueOpen._3._2)))

        if (issueDoc.closed_at.contains("null")) {
          klocListTemp
        } else {
          val startCheckClosed: Option[Instant] = getIssuesKlocDate(groupBy, klocListTemp, issueDoc.closed_at)
          if (startCheckClosed.isDefined) {
          val mapValueClose = klocListTemp.get(startCheckClosed.get).get

          //val closedDate1 =  mapValueClose._3._2 else mapValueClose._3._2 + 1
          klocListTemp + (startCheckClosed.get ->(mapValueClose._1, mapValueClose._2, (mapValueClose._3._1, mapValueClose._3._2 + 1)))
        }else
            klocListTemp
        }
      }else{
          klocList1
        }

      }}

    }}.toList.sortBy(_._1)

    val jsonifyRes1 = finalRes.foldLeft(Nil: Iterable[LocIssue]){(x,y) => {
      val totalRange = (Duration.between(y._1,y._2._1).toMillis).toDouble/1000
      x ++ List(LocIssue(y._1.toString, y._2._1.toString,((y._2._2)/1000)/totalRange,IssueState(y._2._3._1, y._2._3._2,0,0))).toIterable
    }}

    val jsonifyRes = jsonifyRes1.foldLeft(Nil: Iterable[LocIssue]){(x,y) => {
    val t = x.toIterator.duplicate
      if(!t._1.isEmpty){
        val z = t._2.toIterator.duplicate
        val f = z._1.toIterable.last
        z._2.toIterable++ List(LocIssue(y.startDate,y.endDate,y.kloc,IssueState(y.issues.Open,y.issues.Close,f.issues.openCumulative+y.issues.Open-y.issues.Close,
          f.issues.closedCumulative+y.issues.Close)))
      }else{
        t._2.toIterable++ List(LocIssue(y.startDate,y.endDate,y.kloc,IssueState(y.issues.Open,y.issues.Close,y.issues.Open-y.issues.Close,
          y.issues.Close)))
      }
    }}

    import JProtocol._
    jsonifyRes.toJson

  }


  def getIssuesKlocDate(groupBy: String, klocList1: Map[Instant, (Instant, Double, (Int, Int))], issueDocDate: String): Option[Instant] = {
    val inst = Instant.parse(issueDocDate)
    val ldt = ZonedDateTime.ofInstant(inst, ZoneId.of("UTC"))

    val startDate2 =
      if (groupBy.equals("week")) {
        //weekly
        inst.minus(Duration.ofDays(ldt.getDayOfWeek.getValue - 1))
      } else {
        //monthly
        ldt.`with`(TemporalAdjusters.firstDayOfMonth()).toInstant //inst.minus(Duration.ofDays(ldt.getDayOfMonth-1))
      }

    val startDate1 = ZonedDateTime.ofInstant(startDate2, ZoneId.of("UTC")).withHour(0).withMinute(0).withSecond(0).toInstant
    val startD = klocList1.keys.filter(x => {
      x.toString.contains(startDate1.toString.substring(0, 11))
    })
    val resultIns = startD.toList
    if(resultIns.isEmpty)
      None
    else
      Some(resultIns.head)
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
      val startDateForFile = documentLis(0).date
      val (inststartDateForFile: Instant, dateRangeList: List[(Instant, Instant, Double, (Int, Int))]) = getDateRangeList(groupBy, startDateForFile)

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
    commitCount.groupBy(_._1) map(y => (y._1,{
      val rangeLoc = y._2.foldLeft(0D)((acc,z) => acc+z._3)
      (y._2(0)._2,rangeLoc,y._2(0)._4)
    }))

  }

  def getDateRangeList(groupBy: String, startDateForFile: String): (Instant, List[(Instant, Instant, Double, (Int, Int))]) = {
    // documentLis is the list of documents in the collection. NOTE that these documents are all sorted in ascending order !!!!
    //transform sorted document list into list of sorted dateRange for the file(document in the DB)
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
      //if (daysBtw % 7 != 0)
      (daysBtw / 7).toInt + 1
      /*else
          (daysBtw / 7).toInt*/
    } else {
      //monthly
      val zonedStart = ZonedDateTime.ofInstant(startDate_groupBy_file, ZoneId.of("UTC"))
      val zonedNow = ZonedDateTime.ofInstant(now, ZoneId.of("UTC"))
      import java.time.temporal.ChronoUnit
      ChronoUnit.MONTHS.between(zonedStart, zonedNow).toInt + 1
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
    (inststartDateForFile, dateRangeList)
  }

  def getMTTF(user: String, repo: String, branch:String, groupBy: String, startDateOfProject: Instant, endDateForProject:Instant): JsValue ={

    val db = mongoCasbah(user+"_"+repo+"_Issues")
    val collections = db.collectionNames()
    val filteredCol = collections.filter(!_.equals("system.indexes")).filter(!_.contains("system_indexes_defect_density"))

    // issues dateRangeList for repo
    val commitCount = filteredCol.flatMap(coll => {
      val eachColl = db(coll)
      val documentLis = eachColl.find().sort(MongoDBObject("date" -> 1)).toList map (y => {
        IssueInfo(y.getAs[String]("date") get, y.getAs[String]("state") get, y.getAs[String]("created_at") get,y.getAs[String]("closed_at") get)
      })
      val startDateForFile = documentLis(0).date
      val (inststartDateForFile: Instant, dateRangeList: List[(Instant, Instant, Double, (Int, Int))]) = getDateRangeList(groupBy, startDateForFile)


      var previousLoc = 0
      val lastDateForIssues = Instant.parse(documentLis(documentLis.length - 1).date)
      val finalres = dateRangeList.map(x => {
        //calculated MTTF for issues that fall in the date range
        val issueMTTFForRange = documentLis.foldLeft(0.0) { (mttf,dbVals) => {
          val issueCreatedDate = Instant.parse(dbVals.created_at)
          val issueClosedDate = if(!dbVals.closed_at.equals("null")) Instant.parse(dbVals.closed_at) else Instant.now()
          if(issueCreatedDate.isBefore(x._1) && issueClosedDate.isAfter(x._1)){

            if(issueClosedDate.isBefore(x._2))
               mttf+ java.time.Duration.between(x._1,issueClosedDate).toMillis / 1000.0
            else
               mttf+ java.time.Duration.between(x._1,x._2).toMillis / 1000.0

          } else if (issueCreatedDate.isBefore(x._2) && issueClosedDate.isAfter(x._1)){

            if(issueClosedDate.isBefore(x._2))
              mttf+ java.time.Duration.between(issueCreatedDate,issueClosedDate).toMillis / 1000.0
            else
              mttf+ java.time.Duration.between(issueCreatedDate,x._2).toMillis / 1000.0

          }else{
            mttf
          }
        }
        }
        val totalDuration = java.time.Duration.between(startDateOfProject,endDateForProject).toMillis / 1000
        val issueSpoilage = issueMTTFForRange/totalDuration
        (x._1, x._2, issueSpoilage)
      })
      finalres

    }).toList.sortBy(_._1)
    val jsonifyRes = commitCount.groupBy(_._1) map(y => {
      val issueSpoilageAcc = y._2.foldLeft(0D)((acc,z) => acc+z._3)
      IssueSpoilage(y._1.toString, y._2(0)._2.toString,issueSpoilageAcc)
    })
    import SpoilageJProtocol._
    jsonifyRes.toList.sortBy(_.startDate).toJson

  }

  def getProjectStartEndDate(dbName:String): (Instant,Instant) ={
    val db = mongoCasbah(dbName)
    val collections = db.collectionNames()
    val filteredCol = collections.filter(!_.equals("system.indexes")).filter(!_.contains("system_indexes_defect_density"))
    println("Filtered collections "+filteredCol.size)
    val firstCollection = filteredCol.toList.sorted
    val start = Instant.parse(db(firstCollection.head).find().sort(MongoDBObject("date" -> 1)).toList(0).getAs[String]("date").get)
    val end = Instant.parse(db(firstCollection.last).find().sort(MongoDBObject("date" -> -1)).toList(0).getAs[String]("date").get)
    (start, end)
  }

  def dataForMetrics(user: String, repo: String, branch:String, groupBy: String): String ={

    log.info("Storing defect density results.")
    val kloc = getKloc(user+"_"+repo+"_"+branch, groupBy)
    val writer = new PrintWriter(new java.io.File("store.txt"))
    kloc.toList.sortBy(_._1)
    val defectDensityResult = getIssues(user,repo,branch, groupBy, kloc)
    dbStore(DefectDensity(defectDensityResult, mongoCasbah(user + "_" + repo + "_" + branch+"_1"),groupBy))
    log.info("Defect density results stored.")

    log.info("Storing spoilage results.")
    val (startDateForSpoilage:Instant, endDateForSpoilage:Instant) = getProjectStartEndDate(user+"_"+repo+"_"+branch+"_URL")
    val spoilageResult: JsValue = getMTTF(user,repo,branch, groupBy,startDateForSpoilage, endDateForSpoilage)

    dbStore(Spoilage(spoilageResult, mongoCasbah(user + "_" + repo + "_" + branch+"_1"),groupBy))


  }

  def storeRepoName(docName: String): String={
    dbStore(RepoNames(mongoCasbah("GitTracking"),docName))
  }



}



