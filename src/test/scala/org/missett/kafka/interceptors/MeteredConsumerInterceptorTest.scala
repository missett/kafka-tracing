package org.missett.kafka.interceptors

import java.util.Properties
import java.util.concurrent.Executors

import cats.effect.{IO, Resource}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.Stores
import org.missett.kafka.interceptors.jaeger.Config.ConfigProps
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

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

  def getTopologyBuilder(input: String, output: String): StreamsBuilder = {
    val builder = new StreamsBuilder()
    val stream = builder.stream[String, Int](input)
    stream.peek((k, v) => println(s"received [$k] -> [$v]")).mapValues(_ * 2).to(output)
    builder
  }
}

class MeteredConsumerInterceptorTest extends FlatSpec with Matchers with EmbeddedKafkaStreams {
  behavior of "MeteredConsumerInterceptor"

  it should "create a topology that links multiple spans together" in new TestTopology {
    val threadcount = 4
    val appindexes = (1 to threadcount).toList

    val all = List.fill(threadcount + 1)(Random.alphanumeric.take(6).toList.mkString)
    val topics = all.sliding(2)
      .map { case List(input, output) => (input, output) }.toList
    val builders = topics.map { case (input, output) => getTopologyBuilder(input, output)}

    implicit val KafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadcount))
    implicit val cs = IO.contextShift(ec)

    runStreams(all, new StreamsBuilder().build(), Map()) {
      publishToKafka(all.head, key = "key", message = 1)

      val resources = builders.zip(appindexes).map { case (builder, appindex) => {
        Resource.make[IO, KafkaStreams]({
          IO {
            val props = new Properties()

            ConfigFactory.load().getConfig("streams").entrySet().iterator().asScala
              .foreach(entry => { props.put(entry.getKey, entry.getValue.unwrapped()) })
            val appid = s"app-$appindex"
            props.put("application.id", appid)
            props.put(ConfigProps.SERVICE_NAME, appid)

            val streams = new KafkaStreams(builder.build(), props)

            streams.start()

            streams
          }
        })({
          s => IO { s.close() }
        })
      } }

      val threads = resources.zip(topics).map { case (res, ts) => ts match { case (_, output) => res.use[IO, Unit](_ => {
        consumeNumberMessagesFromTopics[Int](Set(output), 1, timeout = 60.seconds)

        IO.unit
      }) }}

      threads.parSequence.unsafeRunSync()
    }

//    val mbs = ManagementFactory.getPlatformMBeanServer
//
//    val queueTimeNanosCounterName = new ObjectName("org.missett.kafka.interceptors:type=QueueTimeNanosCounter,topic=input-topic,partition=0")
//    mbs.getAttribute(queueTimeNanosCounterName, "QueueTimeNanos")
//
//    val messagesIngestedCounterName = new ObjectName("org.missett.kafka.interceptors:type=MessagesIngestedCounter,topic=input-topic,partition=0")
//    mbs.getAttribute(messagesIngestedCounterName, "MessagesIngested")
  }
}