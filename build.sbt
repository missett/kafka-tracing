import ReleaseTransformations._

name := "kafka-tracing"
scalaVersion := "2.12.11"
organization := "org.missett.kafka"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

val kafkaVersion = "2.2.0"

val deps = Seq(
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
)
  .map(_ exclude("javax.ws.rs", "javax.ws.rs-api"))
  .map(_ exclude("org.slf4j", "slf4j-log4j12"))

libraryDependencies ++= deps

scalacOptions ++= ScalaCOptions.opts

assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)

