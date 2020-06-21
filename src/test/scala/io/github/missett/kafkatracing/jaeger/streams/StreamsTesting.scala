package io.github.missett.kafkatracing.jaeger.streams

import java.util.Properties

import com.typesafe.config.ConfigFactory
import io.jaegertracing.internal.JaegerTracer
import io.jaegertracing.internal.reporters.InMemoryReporter
import io.jaegertracing.internal.samplers.ConstSampler
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.MockProcessorContext

import scala.collection.JavaConverters._

trait StreamsTesting {
  val reporter = new InMemoryReporter
  val sampler = new ConstSampler(true)

  implicit val tracer = new JaegerTracer.Builder("service")
    .withReporter(reporter)
    .withSampler(sampler)
    .build()

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
