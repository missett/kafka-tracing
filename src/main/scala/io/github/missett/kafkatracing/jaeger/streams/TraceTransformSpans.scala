package io.github.missett.kafkatracing.jaeger.streams

import java.util.concurrent.ConcurrentHashMap

import io.opentracing.Span
import org.apache.kafka.streams.processor.ProcessorContext

object TraceTransformSpans {
  type SpanKey = (String, String, Int, Long)

  class SpanMap extends ConcurrentHashMap[SpanKey, Span] {
    def getOption(key: SpanKey): Option[Span] = Option(super.get(key))
  }

  def getSpanKeyForContext(context: ProcessorContext, operation: String) =
    (operation, context.topic(), context.partition(), context.offset())

  val spans = new SpanMap
}