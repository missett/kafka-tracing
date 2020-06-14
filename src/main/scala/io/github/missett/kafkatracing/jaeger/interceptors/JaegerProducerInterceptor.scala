package io.github.missett.kafkatracing.jaeger.interceptors

import java.util

import io.github.missett.kafkatracing.jaeger.Config
import io.github.missett.kafkatracing.jaeger.model.FollowsFrom
import io.github.missett.kafkatracing.jaeger.model.KafkaSpanOps.{KafkaSpan, _}
import io.jaegertracing.internal.JaegerTracer
import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class JaegerProducerInterceptor extends ProducerInterceptor[Array[Byte], Array[Byte]] {
  implicit var tracer: JaegerTracer = _

  override def onSend(record: ProducerRecord[Array[Byte], Array[Byte]]): ProducerRecord[Array[Byte], Array[Byte]] = {
    val topic = record.topic()
    val partition = Option(record.partition()).map(_.toString).orNull

    if (!topic.endsWith("-changelog") && !topic.endsWith("-repartition")) {
      val tags = List(("partition", partition), ("topic", topic))
      KafkaSpan("produce", tags, record.headers(), FollowsFrom).instant
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
