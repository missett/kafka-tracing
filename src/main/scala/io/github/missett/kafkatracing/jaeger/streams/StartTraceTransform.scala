package io.github.missett.kafkatracing.jaeger.streams

import io.github.missett.kafkatracing.jaeger.model.FollowsFrom
import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps.{KafkaSpanFactory, _}
import io.jaegertracing.internal.JaegerTracer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext

class StartTraceTransform[K, V](operation: String)(implicit tracer: JaegerTracer) extends Transformer[K, V, KeyValue[K, V]] {
  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(key: K, value: V): KeyValue[K, V] = {
    TraceTransformSpans.spans.put(
      TraceTransformSpans.getSpanKeyForContext(this.context, operation),
      KafkaSpanFactory(operation, List.empty[(String, String)], this.context.headers(), FollowsFrom).startSpan
    )

    new KeyValue(key, value)
  }

  override def close(): Unit = {
    tracer.close()
  }
}

class StartTraceTransformSupplier[K, V](operation: String)(implicit tracer: JaegerTracer) extends TransformerSupplier[K, V, KeyValue[K, V]] {
  override def get(): Transformer[K, V, KeyValue[K, V]] = new StartTraceTransform(operation)
}
