package io.github.missett.kafkatracing.serialization

import java.nio.charset.StandardCharsets
import java.{lang, util}

import io.opentelemetry.context.propagation.TextMapPropagator
import org.apache.kafka.common.header.Headers

import scala.collection.JavaConverters._

class HeaderKeyIterable(headers: Headers) extends lang.Iterable[String] {
  override def iterator(): util.Iterator[String] =
    headers.toArray.map(_.key()).toIterator.asJava
}

class KafkaPropagatorGetter extends TextMapPropagator.Getter[Headers] {
  override def keys(carrier: Headers): lang.Iterable[String] =
    new HeaderKeyIterable(carrier)

  override def get(carrier: Headers, key: String): String = {
    val header = carrier.lastHeader(key)

    if (header != null) {
      new String(header.value(), StandardCharsets.UTF_8)
    } else {
      null
    }
  }
}
