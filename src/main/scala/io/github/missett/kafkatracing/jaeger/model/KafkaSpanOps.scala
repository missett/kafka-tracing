package io.github.missett.kafkatracing.jaeger.model

import java.nio.charset.StandardCharsets
import java.util
import java.util.Map

import io.opentracing.propagation.TextMap
import org.apache.kafka.common.header.Headers

import scala.collection.JavaConverters._

object KafkaSpanOps {
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

  implicit case object KafkaContextCoderOps extends ContextCoderOps[Headers] {
    override def injector(headers: Headers): TextMap = new ContextHeaderEncoder(headers)
    override def extractor(headers: Headers): TextMap = new ContextHeaderEncoder(headers)
  }

  case class KafkaSpan(operation: String, tags: List[(String, String)], context: Headers, ref: SpanReference) extends Span[Headers]
}
