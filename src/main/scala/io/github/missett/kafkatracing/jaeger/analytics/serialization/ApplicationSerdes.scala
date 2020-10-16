package io.github.missett.kafkatracing.jaeger.analytics.serialization

import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.Config
import io.github.missett.kafkatracing.jaeger.analytics.model.Span
import io.github.missett.kafkatracing.jaeger.analytics.serialization.JsonSerialization.JsonSerde
import org.apache.kafka.common.serialization.{Serde, Serdes}

trait ApplicationSerdes extends Config {
  import io.circe.generic.auto._

  implicit val StringSerde: Serde[String] = Serdes.String()
  implicit val SpanSerde: JsonSerde[Span] = new JsonSerde[Span]()

}