package io.github.missett.kafkatracing.jaeger.streams

import io.jaegertracing.internal.JaegerSpan
import org.apache.kafka.streams.KeyValue
import org.scalatest.{FlatSpec, Matchers}

class EndTraceTransformTest extends FlatSpec with Matchers {
  behavior of "EndTraceTransformer"

  it should "forward the input without changing it" in new StreamsTesting {
    ender.transform("key", "value") should equal (new KeyValue("key", "value"))
  }

  it should "end an existing trace that was started by the StartTransform" in new StreamsTesting {
    starter.transform("key", "value")

    val key = TraceTransformSpans.getSpanKeyForContext(starter.context, operation)

    TraceTransformSpans.spans.size() should equal (1)
    TraceTransformSpans.spans.get(key) should not be (null)
    TraceTransformSpans.spans.get(key).asInstanceOf[JaegerSpan].isFinished should equal (false)

    ender.transform("key", "value")

    reporter.getSpans.size() should equal (1)
    reporter.getSpans.get(0).isFinished should equal (true)
  }

  it should "remove an ended trace from the span map" in new StreamsTesting {
    starter.transform("key", "value")

    val key = TraceTransformSpans.getSpanKeyForContext(starter.context, operation)

    TraceTransformSpans.spans.size() should equal (1)
    TraceTransformSpans.spans.get(key) should not be (null)

    ender.transform("key", "value")

    TraceTransformSpans.spans.size() should equal (0)
    TraceTransformSpans.spans.get(key) should be (null)
  }

  it should "do nothing if a span is not found at the key" in new StreamsTesting {
    TraceTransformSpans.spans.size() should equal (0)
    TraceTransformSpans.spans.get(key) should be (null)

    ender.transform("key", "value")

    TraceTransformSpans.spans.size() should equal (0)
    TraceTransformSpans.spans.get(key) should be (null)
  }
}