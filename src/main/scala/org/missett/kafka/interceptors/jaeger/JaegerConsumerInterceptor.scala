package org.missett.kafka.interceptors.jaeger

import java.util

import io.jaegertracing.internal.{JaegerSpan, JaegerTracer}
import io.opentracing.References
import io.opentracing.propagation.Format
import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

class JaegerConsumerInterceptor extends ConsumerInterceptor[Array[Byte], Array[Byte]] {
  var tracer: JaegerTracer = _

  val spans = scala.collection.mutable.Map.empty[String, JaegerSpan]

  private def getSpanKey(topic: String, partition: Int, offset: Long) = s"$topic-$partition-$offset"

  override def onConsume(records: ConsumerRecords[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val it = records.iterator()

    while(it.hasNext) {
      val record = it.next()
      val offset = record.offset()
      val partition = record.partition()
      val topic = record.topic()

      val builder = tracer.buildSpan("consume-and-commit")
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

      spans.put(getSpanKey(topic, partition, offset), span)
    }

    records
  }

  override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    offsets.asScala.foreach { case (tp, om) =>
      // The offsets returned here are the new offsets for the consumer after processing the
      // message, so to find the offset of the original message that started the trace we subtract one

      val key = getSpanKey(tp.topic(), tp.partition(), om.offset() - 1)

      spans.get(key).foreach(span => {
        span.finish()
        spans.remove(key)
      })
    }
  }

  override def close(): Unit = {
    tracer.close()
  }

  override def configure(config: util.Map[String, _]): Unit = {
    tracer = Config.fromConf(config)
  }
}