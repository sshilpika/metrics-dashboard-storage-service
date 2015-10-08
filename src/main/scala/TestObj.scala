//package edu.luc.cs.metrics.ingestion.service

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
    /*val f = CommitKLocService.getMongoUrl("django","django","master", Option(accessToken))
    f.onComplete{
      case Success(v) => println(v.length+" is the number of commits")
      case Failure(v) => println("FAILED")
    }
    Await.result(f,1000 seconds)*/
    val timeout = Timeout(1 hour)
    val f3 = CommitKLocService.sortLoc("Abjad","abjad","master", Option(accessToken))
    println("Done"+f3.length)
    /*f3.onComplete {
      case Success(v) => println("Loc done!"+v.length)
      actorsys.shutdown()
      case Failure(v) => println("Loc and range calculations failed")
        v.printStackTrace()
        actorsys.shutdown()
    }
    Await.ready(f3, 15 minutes)*/
    /*val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => {
      println(x + " " + y); x + y
    }
    }
    println("COUNTS:")
    counts.saveAsTextFile("counts.txt")
    /**/def storeCommitInfo(ingestStrategy: IngestionStrategy, page: Option[String]): Future[String]={

    implicit val timeout = ingestStrategy.timeout
    ingestStrategy.rawHeaderList
    val url = if(page.isDefined){
      new URL(ingestStrategy.url,page.get} else "https://api.github.com/repos/"+user+"/"+repo+"/issues"

  }*/
  }

}
