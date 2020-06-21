package io.github.missett.kafkatracing.jaeger.streams

import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps.{KafkaSpanFactory, _}
import io.github.missett.kafkatracing.jaeger.model.NoReference
import io.opentracing.References
import org.apache.kafka.streams.KeyValue
import org.scalatest.{FlatSpec, Matchers}

class StartTraceTransformTest extends FlatSpec with Matchers {
  behavior of "StartTraceTransform"

  it should "forward the input without changing it" in new StreamsTesting {
    starter.transform("key", "value") should equal (new KeyValue("key", "value"))
  }

  it should "start a span when forwarding a value" in new StreamsTesting {
    starter.transform("key", "value")

    val key = TraceTransformSpans.getSpanKeyForContext(starter.context, operation)

    TraceTransformSpans.spans.size() should equal (1)
    TraceTransformSpans.spans.get(key) should not be (null)
  }

  it should "link the span to an existing context" in new StreamsTesting {
    KafkaSpanFactory("previous", List.empty, headers, NoReference).startAndFinishSpan

    starter.transform("key", "value")

    val key = TraceTransformSpans.getSpanKeyForContext(starter.context, operation)
    TraceTransformSpans.spans.size() should equal (1)

    TraceTransformSpans.spans.get(key).finish()

    reporter.getSpans.size() should equal (2)

    val span = reporter.getSpans.get(1)
    span.getReferences.size() should equal (1)
    val ref = span.getReferences.get(0)
    ref.getType should equal (References.FOLLOWS_FROM)
  }
}
