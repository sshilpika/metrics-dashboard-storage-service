package edu.luc.cs.metrics.ingestion.service

import akka.util.Timeout
import org.apache.spark.{SparkConf, SparkContext}
import concurrent.duration._
import concurrent.ExecutionContext.Implicits._
import scala.concurrent._

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
    val user = input(0)
    val repo = input(1)
    val branch = input(2)
    CommitDensityService.dataForMetrics(user, repo, branch, "week")
    CommitDensityService.dataForMetrics(user, repo, branch, "month")
    log.info("DONE storing commit details and defect density result for "+repo)
    // store db names for tracked dbs
    //log.info("Storing tracked Db name for "+repo)
    //CommitDensityService.storeRepoName(user+"_"+repo+"_"+branch)
    val res = Future("Final Metrics results stored in DB")
    Await.result(res,5 minutes)
    println("done")

  }

}
