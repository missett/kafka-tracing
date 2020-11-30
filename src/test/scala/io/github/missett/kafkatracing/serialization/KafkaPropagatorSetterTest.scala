package io.github.missett.kafkatracing.serialization

import java.nio.charset.StandardCharsets

import org.scalatest.{FlatSpec, Matchers}

class KafkaPropagatorSetterTest extends FlatSpec with Matchers {
  behavior of "KafkaPropagatorSetter"

  it should "set a value in the header" in new SerializationTesting {
    val key = "foo"
    val value = "bar"
    val records = headers(List.empty)
    new KafkaPropagatorSetter().set(records, key, value)
    val result = new String(records.lastHeader(key).value(), StandardCharsets.UTF_8)

    result should equal (value)
  }
}
