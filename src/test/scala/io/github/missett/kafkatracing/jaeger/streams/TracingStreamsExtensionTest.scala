package io.github.missett.kafkatracing.jaeger.streams

import org.scalatest.{FlatSpec, Matchers}
import TracingStreamsExtension._
import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}

class TracingStreamsExtensionTest extends FlatSpec with Matchers {
  behavior of "TracingStreamsExtension"

  it should "allow .trace to be called on a KStream which creates a trace for the given operation" in new StreamsTesting {
    val input = "input-topic"
    val output = "output-topic"
    val op = "double"

    implicit val StringSerde = Serdes.String
    implicit val IntSerde = Serdes.Integer
    implicit val Cons = Consumed.`with`[String, Int]
    implicit val Prod = Produced.`with`[String, Int]

    val result = MockedStreams().topology(builder => {
      builder.stream[String, Int](input).trace(op, _.mapValues(_ * 2)).to(output)
    })
      .input(input, StringSerde, IntSerde, Seq(("key", 2)))
      .output(output, StringSerde, IntSerde, 1)

    result should equal (Seq(("key", 4)))

    reporter.getSpans.size() should equal (1)
    val span = reporter.getSpans.get(0)
    span.getOperationName should equal (op)
    span.isFinished should equal (true)
  }
}