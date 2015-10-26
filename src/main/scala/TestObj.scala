package edu.luc.cs.metrics.ingestion.service

import akka.util.Timeout
import edu.luc.cs.metrics.ingestion.service.CommitKLocService
import edu.luc.cs.metrics.ingestion.service.`package`._
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.Await
import scala.util._
import concurrent.duration._
import concurrent.ExecutionContext.Implicits._
/**
 * Created by shilpika on 7/24/15.
 */
object TestObj {
  def main(args: Array[String]) {

    println("\nEnter username/reponame/branchname/groupBy")
    val lines = scala.io.Source.stdin.getLines
    val input = lines.next().split("/")

    println(s"You entered: \nUsername: ${input(0)} \nReponame: ${input(1)} \nBranchname: ${input(2)}\n")
    val timeout = Timeout(1 hour)
    CommitDensityService.dataForDefectDensity(input(0), input(1), input(2), "week")
    CommitDensityService.dataForDefectDensity(input(0), input(1), input(2), "month")
    CommitDensityService.storeRepoName(input(0)+"_"+input(1)+"_"+input(2))
    println("done")

    /*resultF.onComplete {
      case Success(v) => println("Loc done!"+v.compactPrint.length)
       // actorsys.shutdown()
      case Failure(v) => println("Loc and range calculations failed")
        v.printStackTrace()
        actorsys.shutdown()
    }
    Await.result(resultF,1 hour)*/
  }

}
