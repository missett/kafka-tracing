package io.github.missett.kafkatracing.jaeger.analytics.model

object DataAccess {
  trait StoreRead[K, V] {
    def get(key: K): Option[V]
  }

  trait StoreWrite[K, V] {
    def set(key: K, value: V): V
  }

  trait StoreAccess[K, V] extends StoreRead[K, V] with StoreWrite[K, V]
}