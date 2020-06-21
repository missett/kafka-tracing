package io.github.missett.kafkatracing.jaeger

import java.util.concurrent.ConcurrentHashMap

import io.github.missett.kafkatracing.jaeger.model.FollowsFrom
import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps.{KafkaSpanFactory, _}
import io.jaegertracing.internal.JaegerTracer
import io.opentracing.Span
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.kstream.KStream

object TracingStreamsExtension {
  type SpanKey = (String, String, Int, Long)

  class SpanMap extends ConcurrentHashMap[SpanKey, Span] {
    def getOption(key: SpanKey): Option[Span] = Option(super.get(key))
  }

  def getSpanKeyForContext(context: ProcessorContext, operation: String) =
    (operation, context.topic(), context.partition(), context.offset())

  val spans = new SpanMap

  class StartTraceTransform[K, V](operation: String)(implicit tracer: JaegerTracer) extends Transformer[K, V, KeyValue[K, V]] {
    var context: ProcessorContext = _

    override def init(context: ProcessorContext): Unit = {
      this.context = context
    }

    override def transform(key: K, value: V): KeyValue[K, V] = {
      spans.put(
        getSpanKeyForContext(this.context, operation),
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

  class EndTraceTransform[K, V](operation: String) extends Transformer[K, V, KeyValue[K, V]] {
    var context: ProcessorContext = _

    override def init(context: ProcessorContext): Unit = {
      this.context = context
    }

    override def transform(key: K, value: V): KeyValue[K, V] = {
      val spanKey = getSpanKeyForContext(this.context, operation)
      spans.getOption(spanKey).foreach(_.finish())
      spans.remove(spanKey)
      new KeyValue[K, V](key, value)
    }

    override def close(): Unit = {
      ()
    }
  }

  class EndTraceTransformSupplier[K, V](operation: String) extends TransformerSupplier[K, V, KeyValue[K, V]] {
    override def get(): Transformer[K, V, KeyValue[K, V]] = new EndTraceTransform(operation)
  }

  implicit class TracingStreamsExtensionImp[K, V](streams: KStream[K, V]) {
    def trace(operation: String, f: KStream[K, V] => KStream[K, V])(implicit tracer: JaegerTracer): KStream[K, V] = {
      f(
        streams.transform(new StartTraceTransformSupplier[K, V](operation))
      )
        .transform(new EndTraceTransformSupplier[K, V](operation))
    }
  }
}
