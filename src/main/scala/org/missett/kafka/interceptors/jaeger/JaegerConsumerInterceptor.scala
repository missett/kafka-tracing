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

      val context = tracer.extract(Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(record.headers()))

      if (context != null) {
        builder.addReference(References.FOLLOWS_FROM, context)
      }

      val span = builder.start()

      spans.put(getSpanKey(topic, partition, offset), span)
    }

    records
  }

  override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    offsets.asScala.foreach { case (tp, om) =>
      val key = getSpanKey(tp.topic(), tp.partition(), om.offset())

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