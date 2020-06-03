package org.missett.kafka.interceptors.jaeger

import java.util

import io.jaegertracing.internal.JaegerTracer
import io.opentracing.References
import io.opentracing.propagation.Format
import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class JaegerProducerInterceptor extends ProducerInterceptor[Array[Byte], Array[Byte]] {
  var tracer: JaegerTracer = _

  override def onSend(record: ProducerRecord[Array[Byte], Array[Byte]]): ProducerRecord[Array[Byte], Array[Byte]] = {
    val partition = record.partition()
    val topic = record.topic()

    if (!topic.endsWith("-changelog") && !topic.endsWith("-repartition")) {
      val builder = tracer.buildSpan("produce")
        .withTag("partition", partition)
        .withTag("topic", topic)

      // Extract any existing tracing context from the inbound message and attach a reference to the current (new) span
      val context = tracer.extract(Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(record.headers()))

      if (context != null) {
        builder.addReference(References.FOLLOWS_FROM, context)
      }

      val span = builder.start()

      // Inject context into outbound record headers
      tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(record.headers()))

      // Finish the span immediately, see note attached to onAcknowledgement
      span.finish()
    }

    record
  }

  // We do not finish the span in this method because the acknowledgement behavior varies according to the broker and producer configs
  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = (

  )

  override def close(): Unit = {
    tracer.close()
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    tracer = Config.fromConf(configs)
  }
}
