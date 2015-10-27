package edu.luc.cs.metrics.ingestion.service

/**
 * Created by shilpika on 7/24/15.
 */

import scala.concurrent.Await
import scala.util._
import concurrent.duration._
import concurrent.ExecutionContext.Implicits._

object TrackedRepo {
  def main(args: Array[String]) {

    /*import com.mongodb.casbah.Imports._
    val db = MongoClient("localhost", 27017)
    val dbNames = db.getDatabaseNames()
    val repoDbs = dbNames.toList.filter(_.contains("_URL"))
    val coll = db("GitTracking")("RepoNames")
    repoDbs.map(name => {
      val newName = name.take(name.length-4)
      coll.update(MongoDBObject("repo_name" -> newName),$set("repo_name" -> newName),true,true)
    }) foreach println*/
    while(true){
      gitIngestion
      Thread.sleep(24*60*60*1000)
    }


  }

  def gitIngestion():List[String]={
    import com.mongodb.casbah.Imports._
    val db = MongoClient("localhost", 27017)
    val coll = db("GitTracking")("RepoNames")
    val dbNames = coll.find().toList map(y => {
      y.getAs[String]("repo_name") get})
    dbNames.map(dbName => {
      val repoDetails = dbName.split("_")
      //Issues
      val IssueDbName = dbName.take(dbName.length-6)+"Issues"
      val IssueDB = db(IssueDbName)
      println(IssueDbName)
      val issueCollName = IssueDB.collectionNames().filter(!_.equals("system.indexes")).toList.max
      val issueDoc = IssueDB(issueCollName).findOne(MongoDBObject(),MongoDBObject("date" -> 1),MongoDBObject("date"-> -1))
      val issueDate = issueDoc.map(x => {
        x.getAs[String]("date") get
      }) getOrElse("")

      val urlIssues = "https://api.github.com/repos/"+repoDetails(0)+"/"+repoDetails(1)+"/issues?state=all&since="+issueDate
      val issuesF = commitIssueCollection(repoDetails(0),repoDetails(1),repoDetails(2),"Issues",Option(accessToken_Issues),None,None,urlIssues)

      issuesF.onComplete{
        case Success(v) => println(v)
          //Commits
          val mongoDBName = db(dbName+"_URL")
          val collName = mongoDBName.collectionNames().filter(!_.equals("system.indexes")).toList.max
          val docs = mongoDBName(collName).findOne(MongoDBObject(),MongoDBObject("date" -> 1),MongoDBObject("date"-> -1))
          val date = docs.map(x => {
            x.getAs[String]("date") get
          }) getOrElse("")

          val urlCommits = "https://api.github.com/repos/"+repoDetails(0)+"/"+repoDetails(1)+"/commits?sha="+repoDetails(2)+"&since="+date
          val commitsF = commitIssueCollection(repoDetails(0),repoDetails(1),repoDetails(2),"Commits",Option(accessToken_CommitsURL),None,None,urlCommits)
          commitsF.onComplete {
            case Success(v1) => println(v1)
              val newCollNames = mongoDBName.collectionNames().filter(!_.equals("system.indexes")).toList.sortWith(_ > _).takeWhile(_ >= collName)
              //get MongoUrl for commits
              val f1 = CommitKLocService.getSelectedMongoUrl(dbName,Option(accessToken_CommitsURL),newCollNames)
              f1.onComplete{
                case Success(urlList) =>
                  println("URLLIST:" + urlList.length)
                  // get remaining rate limit
                  val rate = rateLimit.calculateRateLimit(Option(accessToken_CommitsURL))
                  rate.onComplete {
                    case Success(rateVal) =>
                      //grouping the urlLists to avoid Github rate limit abuse
                      val tokens_needed = urlList.length/5000.0
                      if(tokens_needed<= 12 ){

                        val urlGroups =  groupUrlList(urlList)
                        //val urlListGroups = groupListByRateLimit(urlList, rateVal)
                        //Storing the loc info here call is made using groups of url
                        val stack = scala.collection.mutable.Stack[String]()
                        more_tokens.flatMap(x => stack.push(x))
                        var access_token:String = stack.pop

                        urlGroups map(urlLis => {
                          println(urlLis.length + " length of inner List")
                          val index = urlGroups.indexOf(urlLis)
                          //val access_token_temp = access_token
                          println("The index is:"+index)
                          if((index+1) % 5 == 0 ){
                            println("switching tokens! "+index)
                            access_token = stack.pop
                          }
                          println("INDEX????"+index+"ACCESS TOKEN?????"+access_token)
                          val f2 = CommitKLocService.storeCommitKlocInfo(repoDetails(0), repoDetails(1), repoDetails(2), Option(access_token.toString), urlLis)
                          f2.onComplete {
                            case Success(value) => println(s"Successfully stored commit information for ${value.length} files")
                            case Failure(value) => println("Kloc storage failed with message: ")
                              value.printStackTrace()
                              actorsys.shutdown()
                          }

                          Await.result(f2, 1 hour) // wait for result from storing commit KLOC information

                          println("Thread sleep before next call")
                          if(urlGroups.length>1)
                            Thread.sleep(2 * 60*1000)

                        })
                        CommitKLocService.sortLoc(repoDetails(0), repoDetails(1), repoDetails(2)/*, Option(accessToken_CommitsURL)*/)
                        CommitDensityService.dataForDefectDensity(repoDetails(0), repoDetails(1), repoDetails(2), "week")
                        CommitDensityService.dataForDefectDensity(repoDetails(0), repoDetails(1), repoDetails(2), "month")
                        println("DONE storing commit details and defect density result")
                        // store db names for tracked dbs
                        println("Storing tracked Db name")
                        //CommitDensityService.storeRepoName(repoDetails(0)+"_"+repoDetails(1)+"_"+repoDetails(2))

                      }else{
                        println("URL LIST is > 120,000")
                      }
                    case Failure(rateError) => println("rate retrieval failed:" + rateError)
                      rateError.printStackTrace()
                      actorsys.shutdown()
                  }
                  Await.result(rate,1 hour)
                  println("Done processing RATE!!")
                case Failure (v) =>
                println(v)
                actorsys.shutdown()
          }
              Await.result(f1,1 hour)
              println("Done processing urls!!")
            case Failure(v) =>
              println(v)
              actorsys.shutdown()
          }
          Await.result(commitsF,1 hour)
        case Failure(v) => println(v)
          actorsys.shutdown()

          }
      Await.result(issuesF,1 hour)
      println("Next DB")

  })

    dbNames
}

  def groupUrlList(urlList:List[String]):List[List[String]]= {

    urlList.grouped(950).toList
  }

  def groupListByRateLimit(urlList: List[String], rateVal: Int): List[List[String]] = {
    if (rateVal < 1000) {
      val urlListGroup1 = urlList.grouped(rateVal).toList
      urlListGroup1.head :: urlListGroup1.tail.flatten.grouped(1000).toList
    } else
      urlList.grouped(1000).toList
  }

}
