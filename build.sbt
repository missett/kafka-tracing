//import ReleaseTransformations._

organization := "io.github.missett"
name := "kafka-tracing"
scalaVersion := "2.12.11"

scmInfo := Some(
  ScmInfo( url("https://github.com/missett/kafka-tracing"), "https://github.com/missett/kafka-tracing.git" )
)

developers := List(
  Developer( id = "missett", name = "Ryan Missett", email = "nope@nope.com", url = url("https://nope.com") )
)

description := "Tools for tracing distributed kafka systems with Jaeger"
licenses := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/missett/kafka-tracing"))

val kafkaVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "io.jaegertracing" % "jaeger-client" % "1.2.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe" % "config" % "1.4.0",

  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
  "io.github.embeddedkafka" %% "embedded-kafka-streams" % kafkaVersion % Test,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion % Test,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion % Test,
  "org.typelevel" %% "cats-effect" % "2.1.3" % Test,
  "org.typelevel" %% "cats-core" % "2.0.0" % Test,
)
  .map(_ exclude("javax.ws.rs", "javax.ws.rs-api"))
  .map(_ exclude("org.slf4j", "slf4j-log4j12"))

scalacOptions ++= ScalaCOptions.opts

pomIncludeRepository := { _ => false }
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
publishMavenStyle := true

credentials += Credentials(
  "GnuPG Key ID",
  "gpg",
  "09897011459E84E59E67569CCAFCFC429E31D2A2",
  "notused"
)

//Global / PgpKeys.gpgCommand := (baseDirectory.value / "gpg").getAbsolutePath

//releaseProcess := Seq[ReleaseStep](
//  checkSnapshotDependencies,
//  inquireVersions,
//  runClean,
//  runTest,
//  setReleaseVersion,
//  commitReleaseVersion,
//  tagRelease,
//  publishArtifacts,
//  setNextVersion,
//  commitNextVersion
//)
