package edu.luc.cs.metrics.ingestion.service
//mongo pagination!!!

import scala.concurrent.Await
import scala.util.{Failure, Success}
import concurrent.duration._
import concurrent.ExecutionContext.Implicits._

object Boot extends App{

  val lines = scala.io.Source.stdin.getLines
  var choice = ""
  do {

    println("Enter 1 or 2 to choose one from the options below:\n1. Issues\n2. Commits\n")
    choice = lines.next()
  }while(!choice.equals("1") && !choice.equals("2"))

  val metricType = if(choice.toInt ==1) "Issues" else "Commits"

  println("\nEnter username/reponame/branchname")
  val input = lines.next().split("/")

  println(s"You entered: \nUsername: ${input(0)} \nReponame: ${input(1)} \nBranchname: ${input(2)}\n")
  val f = commitIssueCollection(input(0), input(1), input(2),metricType, Option(accessToken_Issues),None, None,"")

  println("Ingestion Service started")

  f.onComplete {
    case Success(value) => println("Success:!!!! " + value)
      if(choice.equals("2")) {
        val f1 = CommitKLocService.getMongoUrl(input(0)+"_"+input(1)+"_"+input(2), Option(accessToken_CommitsURL))
        println("KLOC Service started")
        f1.onComplete {
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
                    val f2 = CommitKLocService.storeCommitKlocInfo(input(0), input(1), input(2), Option(access_token.toString), urlLis)
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
                  CommitKLocService.sortLoc(input(0), input(1), input(2)/*, Option(accessToken_CommitsURL)*/)
                  CommitDensityService.dataForDefectDensity(input(0), input(1), input(2), "week")
                  CommitDensityService.dataForDefectDensity(input(0), input(1), input(2), "month")
                  println("DONE storing commit details and defect density result")
                  // store db names for tracked dbs
                  println("Storing tracked Db name")
                  CommitDensityService.storeRepoName(input(0)+"_"+input(1)+"_"+input(2))

                }else{
                  println("URL LIST is > 120,000")
                }
              case Failure(rateError) => println("rate retrieval failed:" + rateError)
                rateError.printStackTrace()
                actorsys.shutdown()
            }
            Await.result(rate,1 hour)
          case Failure(value) => println("Url collection failed with message: " + value)
            actorsys.shutdown()
        }
        Await.result(f1,1 hour)
      }
     // actorsys.shutdown()
    case Failure(value) => println("Ingestion Failed with message: "+value)
      actorsys.shutdown()
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

  Await.result(f,2 hours)


}