package io.github.missett.kafkatracing.jaeger.streams

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext

class EndTraceTransform[K, V](operation: String) extends Transformer[K, V, KeyValue[K, V]] {
  var context: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  override def transform(key: K, value: V): KeyValue[K, V] = {
    val spanKey = TraceTransformSpans.getSpanKeyForContext(this.context, operation)
    TraceTransformSpans.spans.getOption(spanKey).foreach(_.finish())
    TraceTransformSpans.spans.remove(spanKey)
    new KeyValue[K, V](key, value)
  }

  override def close(): Unit = {
    ()
  }
}

class EndTraceTransformSupplier[K, V](operation: String) extends TransformerSupplier[K, V, KeyValue[K, V]] {
  override def get(): Transformer[K, V, KeyValue[K, V]] = new EndTraceTransform(operation)
}
