package io.github.missett.kafkatracing.jaeger.analytics.serialization

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import org.apache.kafka.streams.state.KeyValueStore

import scala.util.{Failure, Success, Try}

class KafkaStoreAccess[K, V](store: => KeyValueStore[K, V]) extends StoreAccess[K, V] with LazyLogging {
  private def none(error: String, exception: Throwable): Option[V] = {
    logger.warn(error, exception)
    None
  }

  override def get(key: K): Option[V] = Try(store.get(key)) match {
    case Success(value) => Some(value)
    case Failure(exception) => none(s"failed to decode entry at key [$key]", exception)
  }

  override def set(key: K, value: V): V = {
    store.put(key, value); value
  }
}
