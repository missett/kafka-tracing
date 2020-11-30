package io.github.missett.kafkatracing.serialization

import java.nio.charset.StandardCharsets

import io.opentelemetry.context.propagation.TextMapPropagator
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader

class KafkaPropagatorSetter extends TextMapPropagator.Setter[Headers] {
  override def set(carrier: Headers, key: String, value: String): Unit = {
    carrier.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8))); ()
  }
}