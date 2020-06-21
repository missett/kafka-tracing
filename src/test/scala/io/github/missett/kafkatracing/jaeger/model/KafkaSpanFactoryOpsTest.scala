package io.github.missett.kafkatracing.jaeger.model

import io.github.missett.kafkatracing.jaeger.InterceptorTesting
import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps._
import io.opentracing.References
import io.opentracing.propagation.Format
import org.apache.kafka.common.header.internals.RecordHeaders
import org.scalatest.{FlatSpec, Matchers}

class KafkaSpanFactoryOpsTest extends FlatSpec with Matchers {
  behavior of "ContextHeaderEncoder"

  it should "return null when given some empty headers" in new InterceptorTesting {
    val rec = cRecord(key = 1, value = "one")
    val coder = new ContextHeaderEncoder(rec.headers())
    val (_, _, tracer) = getTestComponents

    val result = tracer.extract(Format.Builtin.TEXT_MAP, coder)

    result should equal (null)
  }

  it should "return null when given some headers containing data not related to Jaeger tracing" in new InterceptorTesting {
    val rec = cRecord(key = 1, value = "one")
    rec.headers().add("foo", "bar".getBytes)
    val coder = new ContextHeaderEncoder(rec.headers())
    val (_, _, tracer) = getTestComponents

    val result = tracer.extract(Format.Builtin.TEXT_MAP, coder)

    result should equal (null)
  }

  behavior of "KafkaSpan"

  it should "create a simple span with the correct operation name and tags, and no reference" in new InterceptorTesting {
    implicit val (reporter, _, tracer) = getTestComponents

    KafkaSpanFactory("operation", List(("foo", "bar"), ("bar", "baz")), new RecordHeaders(), NoReference)
      .startSpan.finish

    reporter.getSpans.size() should equal (1)

    val span = reporter.getSpans.get(0)

    span.getOperationName should be ("operation")
    span.getTags.get("foo") should equal ("bar")
    span.getTags.get("bar") should equal ("baz")
    span.getReferences.size() should equal (0)
  }

  it should "create a span that references the previous span with a FollowsFrom relationship" in new InterceptorTesting {
    implicit val (reporter, _, tracer) = getTestComponents

    val headers = new RecordHeaders()

    KafkaSpanFactory("first-operation", List(("foo", "bar"), ("bar", "baz")), headers, FollowsFrom)
      .startSpan.finish

    KafkaSpanFactory("second-operation", List(("foo", "bar"), ("bar", "baz")), headers, FollowsFrom)
      .startSpan.finish

    reporter.getSpans.size() should equal (2)

    val first = reporter.getSpans.get(0)
    first.getReferences.size() should equal (0)

    val second = reporter.getSpans.get(1)
    second.getReferences.size() should equal (1)
    second.getReferences.get(0).getType should equal (References.FOLLOWS_FROM)
    second.getReferences.get(0).getSpanContext.getSpanId should equal (first.context().getSpanId)
  }

  it should "start and immediately finish a span when calling .instant on the trace" in new InterceptorTesting {
    implicit val (reporter, _, tracer) = getTestComponents

    KafkaSpanFactory("operation", List(("foo", "bar"), ("bar", "baz")), new RecordHeaders(), FollowsFrom)
      .startAndFinishSpan

    reporter.getSpans.get(0).isFinished should equal (true)
  }
}