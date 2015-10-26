package edu.luc.cs.metrics.ingestion.service

/**
 * Created by shilpika on 7/24/15.
 */
object `package` {
  import akka.actor.ActorSystem
  import com.mongodb.casbah.Imports._
  import akka.event.Logging


  //Auth-Token
  val homeDir = System.getProperty("user.home")
  val accessToken_Issues = scala.io.Source.fromFile(homeDir+"/githubAccessToken").getLines().next()
  val accessToken_CommitsURL = scala.io.Source.fromFile(homeDir+"/githubAccessToken").getLines().next()
  val more_tokens = scala.io.Source.fromFile(System.getProperty("user.home")+"/token_all").getLines().toList
  //Actor System
  implicit val actorsys = ActorSystem("gitDefectDensity")
  val log = Logging(actorsys, getClass)
  // Database Connection through Casbah
  val mongoClientCasbah1 = MongoClient("localhost", 27017)

  // Reactive Mongo
  import reactivemongo.api._
  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))

  val mongoDriver = new MongoDriver
  val mongoConnection = mongoDriver.connection(List("localhost"))

}
