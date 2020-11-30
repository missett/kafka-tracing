package io.github.missett.kafkatracing.interceptors

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SpanReportingProducerInterceptorTest extends FlatSpec with Matchers {
  behavior of "SpanReportingProducerInterceptor"

  it should "record a 'produce' span when processing a record" in new InterceptorTesting {
    val interceptor = new SpanReportingProducerInterceptor()
    interceptor.configure(confmap)

    produce(
      producable("key", "value", partition = 1, topic = topic),
      interceptor
    )

    val span = exporter.getFinishedSpanItems.asScala.toList.head

    span.getName should equal ("produce")
    span.getAttributes.get(AttributeKey.stringKey("partition")) should equal ("1")
    span.getAttributes.get(AttributeKey.stringKey("topic")) should equal (topic)
    span.getKind should equal (Span.Kind.PRODUCER)
    span.hasEnded should equal (true)
  }

  it should "report multiple spans chained together as part of a single trace" in new InterceptorTesting {
    val interceptor = new SpanReportingProducerInterceptor()
    interceptor.configure(confmap)

    val rec = producable("key", "value", partition = 1, topic = topic)

    produce(rec, interceptor)
    produce(rec, interceptor)
    produce(rec, interceptor)

    val List(one, two, three) = exporter.getFinishedSpanItems.asScala.toList

    one.getSpanId should equal (two.getParentSpanId)
    two.getSpanId should equal (three.getParentSpanId)

    List(one, two, three).map(_.getTraceId) should equal (List.fill(3)(one.getTraceId))
  }
}
