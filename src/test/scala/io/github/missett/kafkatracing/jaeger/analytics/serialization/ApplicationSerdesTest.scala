package io.github.missett.kafkatracing.jaeger.analytics.serialization

import io.github.missett.kafkatracing.jaeger.analytics.model.RefType._
import io.github.missett.kafkatracing.jaeger.analytics.model.{Process, Reference, Span, Tag}
import org.scalatest.{FlatSpec, Matchers}

class ApplicationSerdesTest extends FlatSpec with Matchers {
  private val serdes: ApplicationSerdes = new ApplicationSerdes {}

  it should "decode a span JSON input with some references to other spans" in {
    val str = """{"traceId":"AAAAAAAAAADnaSVdf706VQ==","spanId":"oeXhOqfDc3E=","operationName":"consume","references":[{"traceId":"AAAAAAAAAADnaSVdf706VQ==","spanId":"cn63SI1Ur48=","refType":"FOLLOWS_FROM"}],"flags":1,"startTime":"2020-10-16T15:09:39.961Z","duration":"0.000042s","tags":[{"key":"topic","vStr":"61jj3v"},{"key":"partition","vStr":"0"},{"key":"offset","vStr":"0"},{"key":"internal.span.format","vStr":"proto"}],"process":{"serviceName":"app-3","tags":[{"key":"hostname","vStr":"SBGML01778"},{"key":"jaeger.version","vStr":"Java-1.2.0"},{"key":"ip","vStr":"172.16.24.57"}]}}"""
    val bytes = serdes.StringSerde.serializer().serialize("topic", str)
    val result = serdes.SpanSerde.deserializer().deserialize("topic", bytes)

    result should equal (Span(
     "AAAAAAAAAADnaSVdf706VQ==",
      "oeXhOqfDc3E=",
      "consume",
      Some(List(
        Reference("AAAAAAAAAADnaSVdf706VQ==", "cn63SI1Ur48=", FOLLOWS_FROM)
      )),
      1,
      "2020-10-16T15:09:39.961Z",
      "0.000042s",
      List(
        Tag("topic", Some("61jj3v")),
        Tag("partition", Some("0")),
        Tag("offset", Some("0")),
        Tag("internal.span.format", Some("proto")),
      ),
      Process("app-3", List(
        Tag("hostname", Some("SBGML01778")),
        Tag("jaeger.version", Some("Java-1.2.0")),
        Tag("ip", Some("172.16.24.57"))
      ))
    ))
  }

  it should "encode a span" in {
    val span = Span(
      "AAAAAAAAAADnaSVdf706VQ==",
      "oeXhOqfDc3E=",
      "consume",
      Some(List(
        Reference("AAAAAAAAAADnaSVdf706VQ==", "cn63SI1Ur48=", FOLLOWS_FROM)
      )),
      1,
      "2020-10-16T15:09:39.961Z",
      "0.000042s",
      List(
        Tag("topic", Some("61jj3v")),
        Tag("partition", Some("0")),
        Tag("offset", Some("0")),
        Tag("internal.span.format", Some("proto")),
      ),
      Process("app-3", List(
        Tag("hostname", Some("SBGML01778")),
        Tag("jaeger.version", Some("Java-1.2.0")),
        Tag("ip", Some("172.16.24.57"))
      ))
    )

    val bytes = serdes.SpanSerde.serializer().serialize("topic", span)
    val str = serdes.StringSerde.deserializer().deserialize("topic", bytes)

    str should equal ("""{"traceId":"AAAAAAAAAADnaSVdf706VQ==","spanId":"oeXhOqfDc3E=","operationName":"consume","references":[{"traceId":"AAAAAAAAAADnaSVdf706VQ==","spanId":"cn63SI1Ur48=","refType":"FOLLOWS_FROM"}],"flags":1,"startTime":"2020-10-16T15:09:39.961Z","duration":"0.000042s","tags":[{"key":"topic","vStr":"61jj3v"},{"key":"partition","vStr":"0"},{"key":"offset","vStr":"0"},{"key":"internal.span.format","vStr":"proto"}],"process":{"serviceName":"app-3","tags":[{"key":"hostname","vStr":"SBGML01778"},{"key":"jaeger.version","vStr":"Java-1.2.0"},{"key":"ip","vStr":"172.16.24.57"}]}}""")
  }
}