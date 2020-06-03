package org.missett.kafka.interceptors.jaeger

import java.util

import io.jaegertracing.internal.JaegerTracer
import io.opentracing.References
import io.opentracing.propagation.Format
import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

class JaegerConsumerInterceptor extends ConsumerInterceptor[Array[Byte], Array[Byte]] {
  var tracer: JaegerTracer = _

  override def onConsume(records: ConsumerRecords[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val it = records.iterator()

    while(it.hasNext) {
      val record = it.next()
      val offset = record.offset()
      val partition = record.partition()
      val topic = record.topic()

      val builder = tracer.buildSpan("consume")
        .withTag("offset", offset)
        .withTag("partition", partition)
        .withTag("topic", topic)

      // Extract any existing tracing context from the inbound message and attach a reference to the current (new) span
      val context = tracer.extract(Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(record.headers()))

      if (context != null) {
        builder.addReference(References.FOLLOWS_FROM, context)
      }

      val span = builder.start()

      // Inject the current tracing context into the record headers (over the top of any existing tracing context)
      tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(record.headers()))

      span.finish()
    }

    records
  }

  // Ideally we would start the trace in the onConsume callback and finish it in here but the onCommit
  // callback seems really flakey. It seems to only be called for some offsets and not at all for others
  // and I can't work out why right now.
  override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = (

  )

  override def close(): Unit = {
    tracer.close()
  }

  override def configure(config: util.Map[String, _]): Unit = {
    tracer = Config.fromConf(config)
  }
}