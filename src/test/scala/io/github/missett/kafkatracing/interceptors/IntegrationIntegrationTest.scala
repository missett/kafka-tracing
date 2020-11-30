package io.github.missett.kafkatracing.interceptors

import java.util.Properties
import java.util.concurrent.Executors

import cats.effect.{IO, Resource}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.state.Stores
//import io.github.missett.kafkatracing.jaeger.Config.ConfigProps
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.JavaConverters._

trait TestTopology {
  implicit val StringSerde = Serdes.String
  implicit val StringSerializer = StringSerde.serializer()
  implicit val StringDeserializer = StringSerde.deserializer()
  implicit val IntSerde = Serdes.Integer
  implicit val IntSerializer = IntSerde.serializer()
  implicit val IntDeserializer = IntSerde.deserializer()
  implicit val Cons = Consumed.`with`[String, Int]
  implicit val Prod = Produced.`with`[String, Int]
  implicit val Group = Grouped.`with`[String, Int]
  implicit val Mat = Materialized.as[String, Int](Stores.inMemoryKeyValueStore("store"))

  val exporter = InMemorySpanExporter.create()

  OpenTelemetrySdk.getGlobalTracerManagement.addSpanProcessor(SimpleSpanProcessor.builder(exporter).build())

  def getLinearTopologyBuilder(input: String, output: String): StreamsBuilder = {
    val builder = new StreamsBuilder()
    val stream = builder.stream[String, Int](input)
    stream.mapValues(_ * 2).to(output)
    builder
  }

  def getBranchingTopologyBuilder(input: String, high: String, low: String): StreamsBuilder = {
    val builder = new StreamsBuilder()
    val stream = builder.stream[String, Int](input)
    stream.mapValues(_ * 5).to(high)
    stream.mapValues(_ * 2).to(low)
    builder
  }

  def getStreams(builder: StreamsBuilder, appid: String): Resource[IO, KafkaStreams] = {
    Resource.make[IO, KafkaStreams]({
      IO {
        val props = new Properties()

        ConfigFactory.load().getConfig("streams").entrySet().iterator().asScala
          .foreach(entry => {
            props.put(entry.getKey, entry.getValue.unwrapped())
          })
        props.put("application.id", appid)

        val streams = new KafkaStreams(builder.build(), props)

        streams.start()

        streams
      }
    })({
      s => IO {
        s.close()
      }
    })
  }
}

class InterceptorIntegrationTest extends FlatSpec with Matchers with EmbeddedKafkaStreams {
  behavior of "MeteredConsumerInterceptor"

  it should "create a topology that links multiple spans together in a linear chain" in new TestTopology {
    val threadcount = 4
    val appindexes = (1 to threadcount).toList

    val all = List.fill(threadcount + 1)(Random.alphanumeric.take(6).toList.mkString)
    val topics = all.sliding(2)
      .map { case List(input, output) => (input, output) }.toList
    val builders = topics.map { case (input, output) => getLinearTopologyBuilder(input, output)}

    implicit val KafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadcount))
    implicit val cs = IO.contextShift(ec)

    runStreams(all, new StreamsBuilder().build(), Map()) {
      publishToKafka(all.head, key = "key", message = 1)

      val resources = builders.zip(appindexes).map { case (builder, appindex) => getStreams(builder, s"app-$appindex")}

      val threads = resources.zip(topics).map { case (res, ts) => ts match { case (_, output) => res.use[IO, Unit](_ => {
        consumeNumberMessagesFromTopics[Int](Set(output), 1, timeout = 60.seconds)

        IO.unit
      }) }}

      threads.parSequence.unsafeRunSync()

      exporter.getFinishedSpanItems.asScala.toList.sliding(2, 1).toList.foreach {
        case (first :: second :: Nil) =>
          second.getParentSpanId should equal (first.getSpanId)
        case _ =>
          throw new Exception("should not happen")
      }
    }
  }

  it should "create a topology that links multiple spans together in a branching graph" in new TestTopology {
    val bottomrowcount = 4

    val bottomrowoutputtopics = List.fill(bottomrowcount * 2)(Random.alphanumeric.take(6).mkString).sliding(2, 2).map { case List(high, low) => (high, low) }.toList
    val bottomrowappids = (1 to bottomrowcount).toList.map(i => s"bottom-row-app-$i")
    val bottomrowinputtopics = List.fill(bottomrowcount)(Random.alphanumeric.take(6).mkString)
    val middlerowoutputtopics = bottomrowinputtopics.sliding(2, 2).map { case List(high, low) => (high, low) }.toList
    val middlerowappids = (1 to bottomrowinputtopics.size / 2).toList.map(i => s"middle-row-app-$i")
    val middlerowinputtopics = List.fill(middlerowappids.size)(Random.alphanumeric.take(6).mkString)
    val toprowoutputtopics = middlerowinputtopics.sliding(2, 2).map { case List(high, low) => (high, low) }.toList
    val toprowappids = (1 to middlerowinputtopics.size / 2).toList.map(i => s"top-row-app-$i")
    val toprowinputtopics = List.fill(toprowappids.size)(Random.alphanumeric.take(6).mkString)

    val bottom = bottomrowappids
      .zip(bottomrowinputtopics).map { case (appid, input) => (appid, input) }
      .zip(bottomrowoutputtopics).map { case (idandinput, output) => idandinput match { case (id, input) => (id, input, output) } }

    val middle = middlerowappids
      .zip(middlerowinputtopics).map { case (appid, input) => (appid, input) }
      .zip(middlerowoutputtopics).map { case (idandinput, output) => idandinput match { case (id, input) => (id, input, output) } }

    val top = toprowappids
      .zip(toprowinputtopics).map { case (appid, input) => (appid, input) }
      .zip(toprowoutputtopics).map { case (idandinput, output) => idandinput match { case (id, input) => (id, input, output) } }

    val data = top ++ middle ++ bottom

    val all = toprowinputtopics ++ middlerowinputtopics ++ bottomrowinputtopics ++ bottomrowoutputtopics.flatMap(t => List(t._1, t._2))

    implicit val KafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(data.size))
    implicit val cs = IO.contextShift(ec)

    runStreams(all, new StreamsBuilder().build(), Map()) {
      publishToKafka(all.head, key = "key", message = 1)

      val resources = data.map { case (id, input, outputs) => {
        getStreams(getBranchingTopologyBuilder(input, outputs._1, outputs._2), id)
      } }

      val threads = resources.zip(data).map { case (res, d) => d match { case (_, _, outputs) => res.use[IO, Unit](_ => {
        consumeNumberMessagesFromTopics[Int](Set(outputs._1, outputs._2), 2, timeout = 60.seconds)

        IO.unit
      }) }}

      threads.parSequence.unsafeRunSync()
    }

  }
}