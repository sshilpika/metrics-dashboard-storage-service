name := "metrics-dashboard-storage-service"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  val sprayJsonV = "1.3.2"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-client"  % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
    "io.spray"            %%  "spray-json"    % sprayJsonV,
    "io.spray"            %%  "spray-testkit" % sprayV  % Test,
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % Test,
    "org.specs2"          %%  "specs2-core"   % "2.3.11" % Test,
    "org.mongodb" %% "casbah" % "2.8.1",
    "org.reactivemongo" %% "reactivemongo" % "0.11.2",
    "org.apache.spark" % "spark-core_2.10" % "1.4.1"

  )
}

enablePlugins(JavaAppPackaging)
    