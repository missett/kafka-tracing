package io.github.missett.kafkatracing.serialization

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class KafkaPropagatorGetterTest extends FlatSpec with Matchers {
  behavior of "KafkaPropagatorGetter"

  it should "be able to retrieve a value in the headers" in new SerializationTesting {
    val key = "foo"
    val value = "bar"
    val result = new KafkaPropagatorGetter().get(headers(List((key, value))), key)

    result should equal (value)
  }

  it should "return all the keys in the headers" in new SerializationTesting {
    val keys = List("foo", "bar", "baz")
    val records = headers(keys.map((_, "value")))
    val result = new KafkaPropagatorGetter().keys(records)

    result.asScala.toList should equal (keys)
  }
}