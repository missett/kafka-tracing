package io.github.missett.kafkatracing.jaeger.analytics.serialization

import org.apache.kafka.streams.state.KeyValueStore
import org.mockito.Mockito.when
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class KafkaStoreAccessTest extends FlatSpec with Matchers with MockitoSugar {
  it should "return a None when the store contains a null value at the key" in {
    val kvstore = mock[KeyValueStore[String, String]]
    when(kvstore.get("key")).thenReturn(null.asInstanceOf[String])
    val store = new KafkaStoreAccess(kvstore)
    store.get("key") should equal (None)
  }
}