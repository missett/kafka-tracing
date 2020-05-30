package org.missett.kafka.interceptors

import java.util

import io.jaegertracing.internal.JaegerTracer
import io.jaegertracing.internal.reporters.InMemoryReporter
import io.jaegertracing.internal.samplers.ConstSampler
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import org.missett.kafka.interceptors.JaegerConsumerInterceptor.ConfigProps._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

trait JaegerConsumerInterceptorTesting {
  type Bytes = Array[Byte]

  val topic = "test-topic"

  val service = "test-service"

  implicit val IntSerde: Serde[Int] = Serdes.Integer

  def consume[K, V](key: K, value: V, interceptor: JaegerConsumerInterceptor, partition: Int = 0, offset: Long = 0)(implicit keyser: Serde[K], valser: Serde[V]): ConsumerRecord[Bytes, Bytes] = {
    val topicpartition = new TopicPartition(topic, partition)
    val records = new util.HashMap[TopicPartition, util.List[ConsumerRecord[Bytes, Bytes]]]()
    val k = keyser.serializer().serialize(topic, key)
    val v = valser.serializer().serialize(topic, value)
    val record = new ConsumerRecord[Bytes, Bytes](topic, partition, offset, k, v)

    records.put(topicpartition, List[ConsumerRecord[Bytes, Bytes]](record).asJava)

    interceptor.onConsume(new ConsumerRecords(records))

    record
  }

  def commit(interceptor: JaegerConsumerInterceptor, partition: Int = 0, offset: Long = 0): TopicPartition = {
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

class JaegerConsumerInterceptorTest extends FlatSpec with Matchers {
  it should "set all config values" in {
    val int = new JaegerConsumerInterceptor()

    val config = new util.HashMap[String, String]
    config.put(SERVICE_NAME, "test-service")
    config.put(TRACER_TAGS, "a=foo,b=bar,c=baz")
    config.put(REPORTER_LOG_SPANS, "true")
    config.put(REPORTER_FLUSH_INTERVAL, "1000")
    config.put(REPORTER_MAX_QUEUE_SIZE, "100")
    config.put(SENDER_HOST, "http://localhost")
    config.put(SENDER_PORT, "8000")
    config.put(SENDER_ENDPOINT, "http://localhost:8080/endpoint")
    config.put(SAMPLER_TYPE, "remote")
    config.put(SAMPLER_PARAM, "1")
    config.put(SAMPLER_HOST_PORT, "localhost:8001")

    int.configure(config)

    int.tracer.getServiceName should equal ("test-service")

    int.tracer.tags().get("a") should equal ("foo")
    int.tracer.tags().get("b") should equal ("bar")
    int.tracer.tags().get("c") should equal ("baz")
  }

  it should "send a basic trace" in {
    val int = new JaegerConsumerInterceptor()

    val config = new util.HashMap[String, String]
    config.put(SERVICE_NAME, "test-service")
    config.put(TRACER_TAGS, "a=foo,b=bar,c=baz")
    config.put(REPORTER_LOG_SPANS, "true")
    config.put(REPORTER_FLUSH_INTERVAL, "1000")
    config.put(REPORTER_MAX_QUEUE_SIZE, "100")
    config.put(SENDER_HOST, "http://localhost")
    config.put(SENDER_PORT, "8000")
    config.put(SENDER_ENDPOINT, "http://localhost:14268/api/traces")
    config.put(SAMPLER_TYPE, "remote")
    config.put(SAMPLER_PARAM, "1")
    config.put(SAMPLER_HOST_PORT, "localhost:8001")

    int.configure(config)

    def ser(i: Int) = Serdes.Integer.serializer().serialize("topic", i)

    type Bytes = Array[Byte]

    val partition = new TopicPartition("topic", 0)

    val records = new util.HashMap[TopicPartition, util.List[ConsumerRecord[Bytes, Bytes]]]()

    records.put(partition, List[ConsumerRecord[Bytes, Bytes]](new ConsumerRecord("topic", 0, 0, ser(1), ser(2))).asJava)

    int.onConsume(new ConsumerRecords( records ))

    val offsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()

    offsets.put(partition, new OffsetAndMetadata(0, ""))

    int.onCommit(offsets)
  }

  it should "report a span correctly when a message is conumed and the offset is committed" in new JaegerConsumerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume[Int, Int](1, 2, int)
    commit(int)

    val span = reporter.getSpans.get(0)

    span.getServiceName should equal (service)
    span.getOperationName should equal ("consume-and-commit")
    span.getTags.get("partition") should equal (0)
    span.getTags.get("offset") should equal (0)
    span.isFinished should equal (true)
  }
}