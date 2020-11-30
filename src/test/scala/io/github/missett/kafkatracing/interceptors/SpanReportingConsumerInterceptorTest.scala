package io.github.missett.kafkatracing.interceptors

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SpanReportingConsumerInterceptorTest extends FlatSpec with Matchers {
  it should "record a 'consume' span when processing a record" in new InterceptorTesting {
    val interceptor = new SpanReportingConsumerInterceptor()
    interceptor.configure(confmap)

    consume(
      consumable("key", "value", partition = 1, offset = 2, topic = topic),
      interceptor
    )

    val span = exporter.getFinishedSpanItems.asScala.toList.head

    span.getName should equal ("consume")
    span.getAttributes.get(AttributeKey.stringKey("partition")) should equal ("1")
    span.getAttributes.get(AttributeKey.stringKey("offset")) should equal ("2")
    span.getAttributes.get(AttributeKey.stringKey("topic")) should equal (topic)
    span.getKind should equal (Span.Kind.CONSUMER)
    span.hasEnded should equal (true)
  }

  it should "report multiple spans chained together as part of a single trace" in new InterceptorTesting {
    val interceptor = new SpanReportingConsumerInterceptor
    interceptor.configure(confmap)

    val rec = consumable("key", "value", partition = 2, offset = 3, topic = topic)

    consume(rec, interceptor)
    consume(rec, interceptor)
    consume(rec, interceptor)

    val List(one, two, three) = exporter.getFinishedSpanItems.asScala.toList

    one.getSpanId should equal (two.getParentSpanId)
    two.getSpanId should equal (three.getParentSpanId)

    List(one, two, three).map(_.getTraceId) should equal (List.fill(3)(one.getTraceId))
  }
}
