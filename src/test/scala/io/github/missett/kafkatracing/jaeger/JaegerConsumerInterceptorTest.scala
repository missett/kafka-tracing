package io.github.missett.kafkatracing.jaeger

import io.opentracing.References
import io.opentracing.propagation.Format
import org.scalatest.{FlatSpec, Matchers}

class JaegerConsumerInterceptorTest extends FlatSpec with Matchers {
  behavior of "JaegerConsumerInterceptor"

  it should "report a span correctly when a message is consumed" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    val rec = cRecord(key = 1, value = 2)

    consume(rec, int)

    val span = reporter.getSpans.get(0)

    span.getServiceName should equal (service)
    span.getOperationName should equal ("consume")
    span.getTags.get("partition") should equal (rec.partition())
    span.getTags.get("offset") should equal (rec.offset())
    span.getTags.get("topic") should equal (rec.topic())
    span.isFinished should equal (true)
  }

  it should "continue a trace that is received in the message headers" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    val rec = cRecord(key = 1, value = "one")

    val parentSpan = tracer.buildSpan("parent-operation").start()
    val parentContext = parentSpan.context()

    tracer.inject(parentContext, Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(rec.headers()))

    val consumed = consume(rec, int)

    val spans = reporter.getSpans
    spans.size() should equal (1)

    val span = spans.get(0)

    val refs = span.getReferences
    refs.size() should equal (1)

    val ref = refs.get(0)
    ref.getType should equal (References.FOLLOWS_FROM)
    ref.getSpanContext.getSpanId should equal (parentContext.getSpanId)

    val newContext = tracer.extract(Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(consumed.headers()))
    newContext.getParentId should equal (parentContext.getSpanId)
  }
}
