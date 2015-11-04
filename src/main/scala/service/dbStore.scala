package edu.luc.cs.metrics.ingestion.service

import java.time.{ZoneId, Instant, ZonedDateTime}
import com.mongodb.casbah.Imports._
import spray.json.JsValue

/**
 * Created by shilpika on 7/29/15.
 */
object dbStore {

  def apply(s: Metric): String = s match {
    case Issues(issueList: List[JsValue], db : MongoDB) =>
      issueList.map(commits => {
        val createdAt = commits.asJsObject.getFields("created_at")(0).compactPrint.replaceAll("\"", "")
        val ldt = ZonedDateTime.ofInstant(Instant.parse(createdAt),ZoneId.of("UTC"))
        val start_date = Instant.parse(createdAt).minus(java.time.Duration.ofDays(ldt.getDayOfWeek.getValue-1))
        val since = ZonedDateTime.ofInstant(start_date,ZoneId.of("UTC"))
        val dayOfyear = if(since.getDayOfYear.toString.length ==1) "00"+since.getDayOfYear else if(since.getDayOfYear.toString.length ==2) "0"+since.getDayOfYear else ""+since.getDayOfYear
        val coll = db("COLL"+ldt.getYear+"ISSUE"+dayOfyear)
        val url = commits.asJsObject.getFields("url")(0).compactPrint.replaceAll("\"","")
        val number = commits.asJsObject.getFields("number")(0).compactPrint.replaceAll("\"","")
        val state = commits.asJsObject.getFields("state")(0).compactPrint.replaceAll("\"","")
        val closed_at = commits.asJsObject.getFields("closed_at")(0).compactPrint.replaceAll("\"","")
        val created_at = commits.asJsObject.getFields("created_at")(0).compactPrint.replaceAll("\"","")
        val id = commits.asJsObject.getFields("id")(0).compactPrint.replaceAll("\"","")
        val title = commits.asJsObject.getFields("title")(0).compactPrint.replaceAll("\"","")
        val body = commits.asJsObject.getFields("body")(0).compactPrint.replaceAll("\"","")
        coll.update(MongoDBObject("id" -> id),$set("id" -> id, "url"-> url, "date" -> createdAt,
          "number"-> number, "state" -> state, "title" -> title, "body" -> body, "closed_at" -> closed_at, "created_at" ->created_at ),true,true)

    })
      "Issues Stored"

    case Commits(commitList: List[JsValue], db: MongoDB) =>

      commitList.map(commits => {
        val date = commits.asJsObject.getFields("commit")(0).asJsObject.getFields("committer")(0).asJsObject.getFields("date")(0).compactPrint.replaceAll("\"", "")
        val ldt = ZonedDateTime.ofInstant(Instant.parse(date),ZoneId.of("UTC"))
        val start_date = Instant.parse(date).minus(java.time.Duration.ofDays(ldt.getDayOfWeek.getValue-1))
        val since = ZonedDateTime.ofInstant(start_date,ZoneId.of("UTC"))
        val dayOfyear = if(since.getDayOfYear.toString.length ==1) "00"+since.getDayOfYear else if(since.getDayOfYear.toString.length ==2) "0"+since.getDayOfYear else ""+since.getDayOfYear
        val coll = db("COLL"+ldt.getYear+"DAY"+dayOfyear)
        val url = commits.asJsObject.getFields("url")(0).compactPrint.replaceAll("\"","")
        //inserts if the date is not present else all the matched documents are updated with multi update set to true
        coll.update(MongoDBObject("date" -> date),$set("url"-> url, "date" -> date),true,true)

    })
      "Commits Url Stored"

    case DefectDensity(defectDensityResult: JsValue, db: MongoDB, groupBy: String) =>


        val coll = db("defect_density_"+groupBy)
        coll.update(MongoDBObject("id" -> "1"),$set("defectDensity" -> defectDensityResult.compactPrint),true,true)
        //coll.insert(MongoDBObject("defectDensity" -> defectDensityResult.compactPrint))

      "Defect Density Stored"
    case Spoilage(spoilageResult: JsValue, db: MongoDB, groupBy: String) =>
      val coll = db("issue_spoilage_"+groupBy)
      coll.update(MongoDBObject("id" -> "2"), $set("issueSpoilage" -> spoilageResult.compactPrint), true, true)

      "Issues Spoilage Stored"

    case RepoNames(db: MongoDB, docName: String) =>

      val coll = db("RepoNames")
      coll.update(MongoDBObject("repo_name" -> docName),$set("repo_name" -> docName),true,true)
     // coll.update(MongoDBObject("id" -> "1"),$set("defectDensity" -> defectDensityResult.compactPrint),true,true)

      "RepoName stored for tracking"
  }

}
