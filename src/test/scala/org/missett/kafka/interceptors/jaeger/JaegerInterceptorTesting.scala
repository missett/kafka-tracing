package org.missett.kafka.interceptors.jaeger

import java.util

import io.jaegertracing.internal.JaegerTracer
import io.jaegertracing.internal.reporters.InMemoryReporter
import io.jaegertracing.internal.samplers.ConstSampler
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes

import scala.collection.JavaConverters._

trait JaegerInterceptorTesting {
  type Bytes = Array[Byte]

  val topic = "test-topic"

  val service = "test-service"

  implicit val IntSerde: Serde[Int] = Serdes.Integer
  implicit val StringSerde: Serde[String] = Serdes.String

  def record[K, V](key: K, value: V, partition: Int = 0, offset: Long = 0, topic: String = topic)(implicit keyser: Serde[K], valser: Serde[V]): ConsumerRecord[Bytes, Bytes] = {
    val k = keyser.serializer().serialize(topic, key)
    val v = valser.serializer().serialize(topic, value)
    new ConsumerRecord[Bytes, Bytes](topic, partition, offset, k, v)
  }

  def consume[K, V](record: ConsumerRecord[Bytes, Bytes], interceptor: JaegerConsumerInterceptor): ConsumerRecord[Bytes, Bytes] = {
    val topicpartition = new TopicPartition(record.topic(), record.partition())
    val records = new util.HashMap[TopicPartition, util.List[ConsumerRecord[Bytes, Bytes]]]()
    records.put(topicpartition, List[ConsumerRecord[Bytes, Bytes]](record).asJava)

    interceptor.onConsume(new ConsumerRecords(records))

    record
  }

  def commit(interceptor: JaegerConsumerInterceptor, topic: String = topic, partition: Int = 0, offset: Long = 0): TopicPartition = {
    val offsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    val topicpartition = new TopicPartition(topic, partition)
    offsets.put(topicpartition, new OffsetAndMetadata(offset, "<none>"))

    interceptor.onCommit(offsets)

    topicpartition
  }

  def getTestComponents: (InMemoryReporter, ConstSampler, JaegerTracer) = {
    val reporter = new InMemoryReporter
    val sampler = new ConstSampler(true)
    val tracer = new JaegerTracer.Builder(service).withReporter(reporter).withSampler(sampler).build()

    (reporter, sampler, tracer)
  }
}