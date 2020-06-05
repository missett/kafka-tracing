package io.github.missett.kafkatracing.jaeger

import io.opentracing.References
import io.opentracing.propagation.Format
import org.scalatest.{FlatSpec, Matchers}

class JaegerProducerInterceptorTest extends FlatSpec with Matchers {
  behavior of "JaegerProducerInterceptor"

  it should "report a span correctly when a message is sent" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerProducerInterceptor
    int.tracer = tracer

    val rec = pRecord(key = "one", value = 1)

    send(int, rec)

    val spans = reporter.getSpans
    spans.size() should equal (1)

    val span = spans.get(0)

    span.getOperationName should equal ("produce")
    span.getServiceName should equal (service)
    span.getTags.get("partition") should equal (rec.partition())
    span.getTags.get("topic") should equal (rec.topic())
    span.isFinished should equal (true)
  }

  it should "continue a trace that is received in the message headers" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerProducerInterceptor
    int.tracer = tracer

    val rec = pRecord(key = "one", value = 1)

    val parentSpan = tracer.buildSpan("parent-operation").start()
    val parentContext = parentSpan.context()

    tracer.inject(parentContext, Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(rec.headers()))

    send(int, rec)

    val spans = reporter.getSpans
    spans.size() should equal (1)

    val span = spans.get(0)

    val refs = span.getReferences
    refs.size() should equal (1)

    val ref = refs.get(0)
    ref.getType should equal (References.FOLLOWS_FROM)
    ref.getSpanContext.getSpanId should equal (parentContext.getSpanId)

    val newContext = tracer.extract(Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(rec.headers()))
    newContext.getParentId should equal (parentContext.getSpanId)
  }

  it should "not create a span when topic ends with -changelog" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerProducerInterceptor
    int.tracer = tracer

    val rec = pRecord(key = "one", value = 1, topic = "test-changelog")

    send(int, rec)

    val spans = reporter.getSpans
    spans.size() should equal (0)
  }

  it should "not create a span when topic ends with -repartition" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerProducerInterceptor
    int.tracer = tracer

    val rec = pRecord(key = "one", value = 1, topic = "test-repartition")

    send(int, rec)

    val spans = reporter.getSpans
    spans.size() should equal (0)
  }
}