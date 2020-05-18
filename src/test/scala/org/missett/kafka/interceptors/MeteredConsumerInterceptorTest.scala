package org.missett.kafka.interceptors

import java.lang.management.ManagementFactory

import javax.management.ObjectName
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.scalatest.{FlatSpec, Matchers}

trait TestTopology {
  val input = "input-topic"
  val output = "output-topic"

  implicit val StringSerde = Serdes.String
  implicit val StringSerializer = StringSerde.serializer()
  implicit val StringDeserializer = StringSerde.deserializer()
  implicit val IntSerde = Serdes.Integer
  implicit val IntSerializer = IntSerde.serializer()
  implicit val IntDeserializer = IntSerde.deserializer()
  implicit val Cons = Consumed.`with`[String, Int]
  implicit val Prod = Produced.`with`[String, Int]

  val builder = new StreamsBuilder()
  builder.stream[String, Int](input).mapValues(_ * 2).to(output)

  val config = Map("consumer.interceptor.classes" -> "org.missett.kafka.interceptors.MeteredConsumerInterceptor")
}

class MeteredConsumerInterceptorTest extends FlatSpec with Matchers with EmbeddedKafkaStreams {
  behavior of "MeteredConsumerInterceptor"

  it should "process messages while creating the configured beans" in new TestTopology {
    runStreams(List(input, output), builder.build(), config) {
      publishToKafka(input, message = 1)
      val result = consumeFirstMessageFrom[Int](output)
      result should equal (2)

      val mbs = ManagementFactory.getPlatformMBeanServer

      val queueTimeNanosCounterName = new ObjectName("org.missett.kafka.interceptors:type=QueueTimeNanosCounter,topic=input-topic,partition=0")
      mbs.getAttribute(queueTimeNanosCounterName, "QueueTimeNanos")

      val messagesIngestedCounterName = new ObjectName("org.missett.kafka.interceptors:type=MessagesIngestedCounter,topic=input-topic,partition=0")
      mbs.getAttribute(messagesIngestedCounterName, "MessagesIngested")
    }
  }
}