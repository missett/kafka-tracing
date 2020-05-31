package org.missett.kafka.interceptors

import java.util

import io.jaegertracing.internal.JaegerTracer
import io.jaegertracing.internal.reporters.InMemoryReporter
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.References
import io.opentracing.propagation.Format
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

class JaegerConsumerInterceptorTest extends FlatSpec with Matchers {
  behavior of "ContextHeaderEncoder"

  it should "return null when given some empty headers" in new JaegerConsumerInterceptorTesting {
    val rec = record(key = 1, value = "one")
    val coder = new ContextHeaderEncoder(rec.headers())
    val (_, _, tracer) = getTestComponents

    val result = tracer.extract(Format.Builtin.TEXT_MAP, coder)

    result should equal (null)
  }

  it should "return null when given some headers containing data not related to Jaeger tracing" in new JaegerConsumerInterceptorTesting {
    val rec = record(key = 1, value = "one")
    rec.headers().add("foo", "bar".getBytes)
    val coder = new ContextHeaderEncoder(rec.headers())
    val (_, _, tracer) = getTestComponents

    val result = tracer.extract(Format.Builtin.TEXT_MAP, coder)

    result should equal (null)
  }

  behavior of "JaegerConsumerInterceptor"

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

  it should "report a span correctly when a message is consumed and the offset is committed" in new JaegerConsumerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    val rec = record(key = 1, value = 2)

    consume[Int, Int](rec, int)
    commit(int)

    val span = reporter.getSpans.get(0)

    span.getServiceName should equal (service)
    span.getOperationName should equal ("consume-and-commit")
    span.getTags.get("partition") should equal (rec.partition())
    span.getTags.get("offset") should equal (rec.offset())
    span.getTags.get("topic") should equal (rec.topic())
    span.isFinished should equal (true)
  }

  it should "track multiple spans being active at the same time on the same partition" in new JaegerConsumerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val one = record(key = 1, value = "one", partition = 1, offset = 1)
    val two = record(key = 2, value = "two", partition = 1, offset = 2)

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume(one, int)
    consume(two, int)

    int.spans.size should equal (2)

    commit(int, one.topic(), one.partition(), one.offset())
    commit(int, two.topic(), two.partition(), two.offset())

    val spans = reporter.getSpans

    spans.size() should equal (2)

    int.spans.size should equal (0)
  }

  it should "track multiple spans being active at the same time for the same offset on many partitions" in new JaegerConsumerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val one = record(key = 1, value = "one", partition = 1, offset = 1)
    val two = record(key = 2, value = "two", partition = 2, offset = 1)
    val three = record(key = 3, value = "three", partition = 3, offset = 1)
    val four = record(key = 4, value = "four", partition = 4, offset = 1)

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume(one, int)
    consume(two, int)
    consume(three, int)
    consume(four, int)

    int.spans.size should equal (4)

    commit(int, one.topic(), one.partition(), one.offset())
    commit(int, two.topic(), two.partition(), two.offset())
    commit(int, three.topic(), three.partition(), three.offset())
    commit(int, four.topic(), four.partition(), four.offset())

    val spans = reporter.getSpans

    spans.size() should equal (4)

    int.spans.size should equal (0)
  }

  it should "track multiple spans being active at the same time for the same offset on the same partition on many topics" in new JaegerConsumerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val one = record(key = 1, value = "one", partition = 1, offset = 1, topic = "one")
    val two = record(key = 2, value = "two", partition = 1, offset = 1, topic = "two")
    val three = record(key = 3, value = "three", partition = 1, offset = 1, topic = "three")
    val four = record(key = 4, value = "four", partition = 1, offset = 1, topic = "four")

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume(one, int)
    consume(two, int)
    consume(three, int)
    consume(four, int)

    int.spans.size should equal (4)

    commit(int, one.topic(), one.partition(), one.offset())
    commit(int, two.topic(), two.partition(), two.offset())
    commit(int, three.topic(), three.partition(), three.offset())
    commit(int, four.topic(), four.partition(), four.offset())

    val spans = reporter.getSpans

    spans.size() should equal (4)

    int.spans.size should equal (0)
  }

  it should "continue a trace that is received in the message headers" in new JaegerConsumerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    val rec = record(key = 1, value = "one")

    val parentSpan = tracer.buildSpan("parent-operation").start()
    val parentContext = parentSpan.context()

    tracer.inject(parentContext, Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(rec.headers()))

    consume(rec, int)
    commit(int)

    val spans = reporter.getSpans
    spans.size() should equal (1)

    val span = spans.get(0)

    val refs = span.getReferences
    refs.size() should equal (1)

    val ref = refs.get(0)
    ref.getType should equal (References.FOLLOWS_FROM)
    ref.getSpanContext.getSpanId should equal (parentContext.getSpanId)
  }
}
