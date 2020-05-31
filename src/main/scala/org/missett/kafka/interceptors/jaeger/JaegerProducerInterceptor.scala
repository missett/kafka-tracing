package org.missett.kafka.interceptors.jaeger

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class JaegerProducerInterceptor extends ProducerInterceptor[Array[Byte], Array[Byte]] {
  override def onSend(record: ProducerRecord[Array[Byte], Array[Byte]]): ProducerRecord[Array[Byte], Array[Byte]] = ???

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = ???

  override def close(): Unit = ???

  override def configure(configs: util.Map[String, _]): Unit = {

  }
}
