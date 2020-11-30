package io.github.missett.kafkatracing.interceptors

import java.util

import io.github.missett.kafkatracing.serialization.{KafkaPropagatorGetter, KafkaPropagatorSetter}
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.api.trace.propagation.HttpTraceContext
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.{ContextPropagators, DefaultContextPropagators}
import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class SpanReportingProducerInterceptor extends ProducerInterceptor[Array[Byte], Array[Byte]] {
  private var tracer: Tracer = _

  private val propagators: ContextPropagators = DefaultContextPropagators.builder()
    .addTextMapPropagator(HttpTraceContext.getInstance())
    .build()

  override def onSend(record: ProducerRecord[Array[Byte], Array[Byte]]): ProducerRecord[Array[Byte], Array[Byte]] = {
    val topic = record.topic()
    val partition = if (record.partition() != null) record.partition().toString else null

    val context = propagators
      .getTextMapPropagator
      .extract(Context.current(), record.headers(), new KafkaPropagatorGetter)

    val span = tracer
      .spanBuilder("produce")
      .setAttribute("topic", topic)
      .setAttribute("partition", partition)
      .setSpanKind(Span.Kind.PRODUCER)
      .setParent(context)
      .startSpan()

    span.makeCurrent()

    propagators
      .getTextMapPropagator
      .inject(Context.current(), record.headers(), new KafkaPropagatorSetter)

    span.`end`()

    record
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {}

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    tracer = OpenTelemetry.getGlobalTracer(configs.get("application.id").asInstanceOf[String])
  }
}