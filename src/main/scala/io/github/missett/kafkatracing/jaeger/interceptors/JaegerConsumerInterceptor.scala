package io.github.missett.kafkatracing.jaeger.interceptors

import java.util

import io.github.missett.kafkatracing.jaeger.Config
import io.github.missett.kafkatracing.jaeger.model.FollowsFrom
import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps.{KafkaSpanFactory, _}
import io.jaegertracing.internal.JaegerTracer
import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

class JaegerConsumerInterceptor extends ConsumerInterceptor[Array[Byte], Array[Byte]] {
  implicit var tracer: JaegerTracer = _

  override def onConsume(records: ConsumerRecords[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val it = records.iterator()

    while(it.hasNext) {
      val record = it.next()
      val (offset, partition, topic) = (record.offset().toString, record.partition().toString, record.topic())
      val tags = List(("offset", offset), ("partition", partition), ("topic", topic))
      KafkaSpanFactory("consume", tags, record.headers(), FollowsFrom).startAndFinishSpan
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