package io.github.missett.kafkatracing.jaeger

import java.util.Properties

import com.typesafe.config.ConfigFactory
import io.github.missett.kafkatracing.jaeger.TracingStreamsExtension.{EndTraceTransform, StartTraceTransform}
import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps.{KafkaSpanFactory, _}
import io.github.missett.kafkatracing.jaeger.model.NoReference
import io.jaegertracing.internal.{JaegerSpan, JaegerTracer}
import io.jaegertracing.internal.reporters.InMemoryReporter
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.References
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.MockProcessorContext
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

trait TracingStreamsExtensionTesting {
  val reporter = new InMemoryReporter
  val sampler = new ConstSampler(true)
  implicit val tracer = new JaegerTracer.Builder("service").withReporter(reporter).withSampler(sampler).build()

  val operation = "operation"
  val starter = new StartTraceTransform[String, String](operation)
  val ender = new EndTraceTransform[String, String](operation)
  val props = new Properties()

  ConfigFactory.load().getConfig("streams")
    .entrySet().asScala.foreach(kv => props.put(kv.getKey, kv.getValue.unwrapped()))

  val context = new MockProcessorContext(props)

  val topic = "topic"
  context.setTopic(topic)
  val partition = 0
  context.setPartition(partition)
  val offset = 0L
  context.setOffset(offset)
  val headers = new RecordHeaders()
  context.setHeaders(headers)

  starter.init(context)
  ender.init(context)
}

class TracingStreamsExtensionTest extends FlatSpec with Matchers {
  behavior of "StartTraceTransform"

  it should "forward the input without changing it" in new TracingStreamsExtensionTesting {
    starter.transform("key", "value") should equal (new KeyValue("key", "value"))
  }

  it should "start a span when forwarding a value" in new TracingStreamsExtensionTesting {
    starter.transform("key", "value")

    val key = TracingStreamsExtension.getSpanKeyForContext(starter.context, operation)

    TracingStreamsExtension.spans.size() should equal (1)
    TracingStreamsExtension.spans.get(key) should not be (null)
  }

  it should "link the span to an existing context" in new TracingStreamsExtensionTesting {
    KafkaSpanFactory("previous", List.empty, headers, NoReference).startAndFinishSpan

    starter.transform("key", "value")

    val key = TracingStreamsExtension.getSpanKeyForContext(starter.context, operation)
    TracingStreamsExtension.spans.size() should equal (1)

    TracingStreamsExtension.spans.get(key).finish()

    reporter.getSpans.size() should equal (2)

    val span = reporter.getSpans.get(1)
    span.getReferences.size() should equal (1)
    val ref = span.getReferences.get(0)
    ref.getType should equal (References.FOLLOWS_FROM)
  }

  behavior of "EndTraceTransformer"

  it should "forward the input without changing it" in new TracingStreamsExtensionTesting {
    ender.transform("key", "value") should equal (new KeyValue("key", "value"))
  }

  it should "end an existing trace that was started by the StartTransform" in new TracingStreamsExtensionTesting {
    starter.transform("key", "value")

    val key = TracingStreamsExtension.getSpanKeyForContext(starter.context, operation)

    TracingStreamsExtension.spans.size() should equal (1)
    TracingStreamsExtension.spans.get(key) should not be (null)
    TracingStreamsExtension.spans.get(key).asInstanceOf[JaegerSpan].isFinished should equal (false)

    ender.transform("key", "value")

    reporter.getSpans.size() should equal (1)
    reporter.getSpans.get(0).isFinished should equal (true)
  }

  it should "remove an ended trace from the span map" in new TracingStreamsExtensionTesting {
    starter.transform("key", "value")

    val key = TracingStreamsExtension.getSpanKeyForContext(starter.context, operation)

    TracingStreamsExtension.spans.size() should equal (1)
    TracingStreamsExtension.spans.get(key) should not be (null)

    ender.transform("key", "value")

    TracingStreamsExtension.spans.size() should equal (0)
    TracingStreamsExtension.spans.get(key) should be (null)
  }

  it should "do nothing if a span is not found at the key" in new TracingStreamsExtensionTesting {
    TracingStreamsExtension.spans.size() should equal (0)
    TracingStreamsExtension.spans.get(key) should be (null)

    ender.transform("key", "value")

    TracingStreamsExtension.spans.size() should equal (0)
    TracingStreamsExtension.spans.get(key) should be (null)
  }
}