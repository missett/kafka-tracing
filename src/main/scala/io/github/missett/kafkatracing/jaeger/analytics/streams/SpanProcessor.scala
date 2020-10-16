package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.Span
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}

class SpanProcessor extends Processor[String, Span] with LazyLogging {
  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = { this.context = context; () }

  override def process(key: String, value: Span): Unit = {
    logger.info(s"recieved span [$key] -> [$value]")
  }

  override def close(): Unit = ()
}

class SpanProcessorSupplier extends ProcessorSupplier[String, Span] {
  override def get(): Processor[String, Span] = new SpanProcessor
}