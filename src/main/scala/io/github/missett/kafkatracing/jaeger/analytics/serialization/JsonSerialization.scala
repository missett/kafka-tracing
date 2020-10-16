package io.github.missett.kafkatracing.jaeger.analytics.serialization

import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import io.circe.parser.decode
import io.circe.syntax._

object JsonSerialization {
  private val sd = Serdes.String().deserializer()

  class JsonDeserializer[A](implicit D: Decoder[A]) extends Deserializer[A] {
    override def deserialize(topic: String, data: Array[Byte]): A = decode[A](sd.deserialize(topic, data)) match {
      case Right(result) => result
      case Left(error) => throw error
    }
  }

  private val ss = Serdes.String().serializer()

  class JsonSerializer[A](implicit E: Encoder[A]) extends Serializer[A] {
    override def serialize(topic: String, data: A): Array[Byte] = ss.serialize(topic, data.asJson.noSpaces)
  }

  class JsonSerde[A](implicit E: Encoder[A], D: Decoder[A]) extends Serde[A] {
    override def serializer(): Serializer[A] = new JsonSerializer[A]()
    override def deserializer(): Deserializer[A] = new JsonDeserializer[A]()
  }
}