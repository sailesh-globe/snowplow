name := "remote-adapter-example"

version := "0.0.1"

scalaVersion := "2.11.12"

lazy val akkaVersion    = "2.5.16"

resolvers ++= Seq(
  "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "org.scalaz" %% "scalaz-core" % "7.0.9",

  "com.snowplowanalytics" %% "iglu-scala-client" % "0.5.0",
  "com.snowplowanalytics" %% "snowplow-common-enrich" % "0.35.0"
)
