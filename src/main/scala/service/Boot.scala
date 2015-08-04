package edu.luc.cs.metrics.ingestion.service


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
  val metricType = if(choice.toInt ==1) "Issues" else "FilePaths"

  println("\nEnter username/reponame/branchname")
  val input = lines.next().split("/")

  println(s"You entered: \nUsername: ${input(0)} \nReponame: ${input(1)} \nBranchname: ${input(2)}\n")
  val f = commitIssueCollection(input(0), input(1), input(2),metricType, Option(accessToken),"", None)

  println("Ingestion Service started")

  f.onComplete {
    case Success(value) => println("Success:!!!! " + value)
      if(choice.equals("2")) {
        val f1 = CommitKLocService.getMongoUrl(input(0), input(1), input(2), Option(accessToken))
        println("KLOC Service started")
        f1.onComplete {
          case Success(urlList) =>
            println("URLLIST:" + urlList.length)
            // get remaining rate limit
            val rate = rateLimit.calculateRateLimit(Option(accessToken))
            rate.onComplete {
              case Success(rateVal) =>
                //grouping the urlLists to avoid Github rate limit abuse
                val urlListGroups = groupListByRateLimit(urlList, rateVal)
                //Storing the loc info here call is made using groups of url
                urlListGroups map(urlLis => {
                  println(urlLis.length + " length of inner List")
                  val f2 = CommitKLocService.storeCommitKlocInfo(input(0), input(1), input(2), Option(accessToken), urlLis)
                  f2.onComplete {
                    case Success(value) => println(s"Successfully stored commit information for ${value.length} files")
                    case Failure(value) => println("Kloc storage failed with message: ")
                      value.printStackTrace()
                      actorsys.shutdown()
                  }

                  Await.result(f2, 1 hour) // wait for result from storing commit KLOC information

                  println("Thread sleep before next call")
                  if (urlLis.length < 1000 && (urlListGroups.indexOf(urlLis) != urlListGroups.length - 1)) {
                    Thread.sleep(60 * 60 * 1000)
                  } else if (urlLis.length == 1000 && (urlListGroups.indexOf(urlLis) != urlListGroups.length - 1))
                    Thread.sleep(15 * 60 * 1000)
                })
                val f3 = CommitKLocService.sortLoc(input(0), input(1), input(2), Option(accessToken))
                f3.onComplete {
                  case Success(v) => println("Loc done!")
                    //actorsys.shutdown()
                  case Failure(v) => println("Loc and range calculations failed")
                    v.printStackTrace()
                    actorsys.shutdown()
                }
                Await.result(f3, 15 minutes)
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
    case Failure(value) => println("Ingestion Failed with message: "+value)
      actorsys.shutdown()
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