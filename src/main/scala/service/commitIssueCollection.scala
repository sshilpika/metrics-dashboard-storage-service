package edu.luc.cs.metrics.ingestion.service

import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.Future
/**
 * Created by sshilpika on 6/30/15.
 */


object commitIssueCollection extends ingestionStrategy with Ingestion {

  def apply(user: String, repo: String, branch: String, metricType: String, accessToken: Option[String], page: Option[String]): Future[String] = {

    metricType match {

      case "Commits" =>
        //implicit val timeout = timeout(60.seconds)
        val gitFileCommitList = getHttpResponse(url("https://api.github.com/repos/"+user+"/"+repo+"/commits?sha="+branch, page), rawHeaderList(accessToken),60.seconds)

        gitFileCommitList.map(gitList => {
          rateLimitCheck(gitList)
          //extracting urls from the commit info obtained and storing urlList in the DB
          dbStore(Commits(gitList.entity.data.asString.parseJson.convertTo[List[JsValue]],mongoCasbah(user+"_"+repo+"_"+branch+"_"+"URL")))
          //the url for next page of commits
          println("nextUrlForCurrentWeek: "+getNextPage(gitList))
          getNextPage(gitList)
        }) flatMap(nextUrl => nextUrl.map(page =>{
          this.apply(user,repo,branch,metricType,accessToken,Option(page))
        }).getOrElse(Future("")))


      case "Issues" =>
        //implicit val timeout = timeout(60.seconds)
        val gitIssueList = getHttpResponse(url("https://api.github.com/repos/"+user+"/"+repo+"/issues", page),rawHeaderList(accessToken),60.seconds)

        gitIssueList.map(issueList => {
          rateLimitCheck(issueList)
          //extracting urls from the commit info obtained and storing urlList in the DB
          dbStore(Issues(issueList.entity.data.asString.parseJson.convertTo[List[JsValue]],mongoCasbah(user+"_"+repo+"_"+"Issues")))
          //the url for next page of commits
          println("nextUrlForCurrentWeek: "+getNextPage(issueList))
          getNextPage(issueList)
        }) flatMap(nextUrl => nextUrl.map(page =>{
          this.apply(user,repo,branch,metricType,accessToken,Option(page))
        }).getOrElse(Future("")))
    }


  }


}
