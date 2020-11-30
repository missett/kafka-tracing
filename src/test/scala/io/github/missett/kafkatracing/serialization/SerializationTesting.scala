package io.github.missett.kafkatracing.serialization

import java.nio.charset.StandardCharsets

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}

trait SerializationTesting {
  def headers(kvs: List[(String, String)]) = new RecordHeaders(
    kvs.map { case (key, value) => new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)) }.toArray[Header]
  )
}