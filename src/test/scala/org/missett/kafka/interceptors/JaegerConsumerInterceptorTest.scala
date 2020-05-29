package org.missett.kafka.interceptors

import java.util

import org.scalatest.{FlatSpec, Matchers}
import JaegerConsumerInterceptor.ConfigProps._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.scala.Serdes

import scala.collection.JavaConverters._

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

  it should "do something" in {
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
}