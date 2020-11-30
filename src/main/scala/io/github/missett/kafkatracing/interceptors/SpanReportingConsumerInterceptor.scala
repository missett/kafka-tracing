package io.github.missett.kafkatracing.interceptors

import java.util

import io.github.missett.kafkatracing.serialization.{KafkaPropagatorGetter, KafkaPropagatorSetter}
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.propagation.HttpTraceContext
import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.{ContextPropagators, DefaultContextPropagators}
import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

class SpanReportingConsumerInterceptor extends ConsumerInterceptor[Array[Byte], Array[Byte]] {
  private var tracer: Tracer = _

  private val propagators: ContextPropagators = DefaultContextPropagators.builder()
    .addTextMapPropagator(HttpTraceContext.getInstance())
    .build()

  override def onConsume(records: ConsumerRecords[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val it = records.iterator()

    while(it.hasNext) {
      val record = it.next()
      val (offset, partition, topic) = (record.offset().toString, record.partition().toString, record.topic())

      val context = propagators
        .getTextMapPropagator
        .extract(Context.current(), record.headers(), new KafkaPropagatorGetter)

      val span = tracer
        .spanBuilder("consume")
        .setAttribute("offset", offset)
        .setAttribute("partition", partition)
        .setAttribute("topic", topic)
        .setSpanKind(Span.Kind.CONSUMER)
        .setParent(context)
        .startSpan()

      span.makeCurrent()

      propagators
        .getTextMapPropagator
        .inject(Context.current(), record.headers(), new KafkaPropagatorSetter)

      span.`end`()
    }

    records
  }

  override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    tracer = OpenTelemetry.getGlobalTracer(configs.get("application.id").asInstanceOf[String])
  }
}