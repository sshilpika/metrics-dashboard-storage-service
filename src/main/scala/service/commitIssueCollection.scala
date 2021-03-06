package edu.luc.cs.metrics.ingestion.service

import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.Future
import java.net.URLEncoder
/**
 * Created by sshilpika on 6/30/15.
 */


object commitIssueCollection extends ingestionStrategy with Ingestion {

  def apply(user: String, repo: String, branch: String, metricType: String, accessToken: Option[String], path:Option[String], page: Option[String], cUrl:String): Future[String] = {

    metricType match {

      case "Commits" =>

        val urlC1 = if(cUrl.isEmpty) "https://api.github.com/repos/"+user+"/"+repo+"/commits?sha="+branch else cUrl

        val gitFileCommitList = getHttpResponse(urlC1, rawHeaderList(accessToken),600.seconds)
        val rateRemaining = gitFileCommitList.map(gitList => rateLimitCheck(gitList))

        rateRemaining.flatMap(rate => rate match {
          case 0 =>
            this.apply(user, repo, branch, "Commits", accessToken, path, page,cUrl)
          case _ =>
            gitFileCommitList.map(gitList => {
              dbStore(Commits(gitList.entity.data.asString.parseJson.convertTo[List[JsValue]], mongoCasbah(user + "_" + repo + "_" + branch + "_" + "URL")))
              getNextPageTemp(gitList)

            }) flatMap (nextUrl => nextUrl.map(page => {
              this.apply(user, repo, branch, "Commits", accessToken, path, Option(page), page)
            }).getOrElse(Future("Commits for "+repo+" saved in DB")))
        })

      case "Issues" =>

        val gitIssueList =
          if(cUrl.isEmpty)
            getHttpResponse(url("https://api.github.com/repos/"+user+"/"+repo+"/issues?state=all", page),rawHeaderList(accessToken),60.seconds)
          else
            getHttpResponse(url(cUrl, page),rawHeaderList(accessToken),60.seconds)

        gitIssueList.map(issueList => {
          rateLimitCheck(issueList)
          //extracting urls from the commit info obtained and storing urlList in the DB
          dbStore(Issues(issueList.entity.data.asString.parseJson.convertTo[List[JsValue]],mongoCasbah(user+"_"+repo+"_"+"Issues")))
          //the url for next page of commits
          getNextPage(issueList)
        }) flatMap(nextUrl => nextUrl.map(page =>{
          this.apply(user,repo,branch,metricType,accessToken,None,Option(page),"")
        }).getOrElse(Future("Issues for "+repo+"  Saved in DB")))
    }


  }


}
