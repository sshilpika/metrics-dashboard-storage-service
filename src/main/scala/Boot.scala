package edu.luc.cs499.scala.gitcommitdensity.service

import akka.io.IO
import akka.actor.{ActorSystem,Props}
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http
import concurrent.duration._
import util.Properties

object Boot extends App{

  implicit val system = ActorSystem("gitCommitDensity")
  val service = system.actorOf(Props[MyServiceActor],"git-file-commit-density")
  implicit val timeout = Timeout(60.seconds)
  val port = Properties.envOrElse("PORT","8080").toInt
  IO(Http) ? Http.Bind(service,interface = "0.0.0.0", port = port)
}