package org.missett.kafka.interceptors.jaeger

import java.nio.charset.StandardCharsets
import java.util
import java.util.Map

import io.opentracing.propagation.TextMap
import org.apache.kafka.common.header.Headers

import scala.collection.JavaConverters._

// Encodes and decodes a Jaeger context into kafka record headers
class ContextHeaderEncoder(headers: Headers) extends TextMap {
  override def put(key: String, value: String): Unit = {
    headers.add(key, value.getBytes(StandardCharsets.UTF_8))
    ()
  }

  override def iterator(): util.Iterator[Map.Entry[String, String]] = {
    headers.iterator().asScala.map(header => new util.AbstractMap.SimpleEntry[String, String](
      header.key(),
      new String(header.value())
    ).asInstanceOf[Map.Entry[String, String]]).asJava
  }
}

