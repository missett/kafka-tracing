package org.missett.kafka.interceptors

import java.nio.charset.StandardCharsets
import java.util
import java.util.Map

import com.typesafe.config.ConfigFactory
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.{ReporterConfiguration, SamplerConfiguration, SenderConfiguration}
import io.jaegertracing.internal.{JaegerSpan, JaegerTracer}
import io.opentracing.propagation.TextMap
import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.missett.kafka.interceptors.JaegerConsumerInterceptor.ConfigProps

import scala.collection.JavaConverters._

object JaegerConsumerInterceptor {
  object ConfigProps {
    // "servicename"
    val SERVICE_NAME = "jaeger.interceptor.service.name"
    // "a=foo,b=bar,c=baz"
    val TRACER_TAGS = "jaeger.interceptor.tracer.tags"
    // "true"
    val REPORTER_LOG_SPANS = "jaeger.interceptor.reporter.log.spans"
    // "1000"
    val REPORTER_FLUSH_INTERVAL = "jaeger.interceptor.reporter.flush.interval"
    // "100"
    val REPORTER_MAX_QUEUE_SIZE = "jaeger.interceptor.max.queue.size"
    // "hostname"
    val SENDER_HOST = "jaeger.interceptor.sender.host"
    // "8080"
    val SENDER_PORT = "jaeger.interceptor.sender.port"
    // "/endpoint"
    val SENDER_ENDPOINT = "jaeger.interceptor.sender.endpoint"
    // "remote"
    val SAMPLER_TYPE = "jaeger.interceptor.sampler.type"
    // "1"
    val SAMPLER_PARAM = "jaeger.interceptor.sampler.param"
    // "hostname:8080"
    val SAMPLER_HOST_PORT = "jaeger.interceptor.sampler.hostport"
  }
}

// Encodes and decodes a Jaeger context into kafka record headers
class ContextHeaderEncoder(headers: Headers) extends TextMap {
  override def put(key: String, value: String): Unit = {
    headers.add(key, value.getBytes(StandardCharsets.UTF_8))
    ()
  }

  override def iterator(): util.Iterator[Map.Entry[String, String]] = {
    headers.iterator().asScala.map(header => new util.AbstractMap.SimpleEntry[String, String](
      header.key(),
      new String(header.value())
    ).asInstanceOf[Map.Entry[String, String]]).asJava
  }
}

class JaegerConsumerInterceptor extends ConsumerInterceptor[Array[Byte], Array[Byte]] {
  var tracer: JaegerTracer = _

  val spans = scala.collection.mutable.Map.empty[String, JaegerSpan]

  private def getSpanCollectionKey(topic: String, partition: Int, offset: Long) = s"$topic-$partition-$offset"

  override def onConsume(records: ConsumerRecords[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val it = records.iterator()

    while(it.hasNext) {
      val record = it.next()
      val offset = record.offset()
      val partition = record.partition()
      val topic = record.topic()

      val span = tracer.buildSpan("consume-and-commit")
        .withTag("offset", offset)
        .withTag("partition", partition)
        .withTag("topic", topic)
        .start()

      spans.put(getSpanCollectionKey(topic, partition, offset), span)
    }

    records
  }

  override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    offsets.asScala.foreach { case (tp, om) =>
      val key = getSpanCollectionKey(tp.topic(), tp.partition(), om.offset())

      spans.get(key).foreach(span => {
        span.finish()
        spans.remove(key)
      })
    }
  }

  override def close(): Unit = {
    tracer.close()
  }

  // We go slightly out of our way to configure a tracer object like this because we want to be able to pass
  // our Jaeger config in to the kafka interceptor configs rather than using environment variables (the kafka
  // configs will be configurable via environment variables anyway).

  override def configure(cfg: util.Map[String, _]): Unit = {
    val kafkaConfigValues = ConfigFactory.parseMap(cfg)

    val service = kafkaConfigValues.getString(ConfigProps.SERVICE_NAME)

    val tags = kafkaConfigValues.getString(ConfigProps.TRACER_TAGS)
      .split(",")
      .map(_.split("="))
      .map { case Array(key, value) => (key, value) }
      .toMap
      .asJava

    val reporterLogSpans = kafkaConfigValues.getBoolean(ConfigProps.REPORTER_LOG_SPANS)
    val reporterFlushInterval = kafkaConfigValues.getInt(ConfigProps.REPORTER_FLUSH_INTERVAL)
    val reporterMaxQueueSize = kafkaConfigValues.getInt(ConfigProps.REPORTER_MAX_QUEUE_SIZE)
    val senderHost = kafkaConfigValues.getString(ConfigProps.SENDER_HOST)
    val senderPort = kafkaConfigValues.getInt(ConfigProps.SENDER_PORT)
    val senderEndpoint = kafkaConfigValues.getString(ConfigProps.SENDER_ENDPOINT)

    val reporter = new ReporterConfiguration()
      .withLogSpans(reporterLogSpans)
      .withFlushInterval(reporterFlushInterval)
      .withMaxQueueSize(reporterMaxQueueSize)
      .withSender(new SenderConfiguration()
        .withAgentHost(senderHost)
        .withAgentPort(senderPort)
        .withEndpoint(senderEndpoint)
      )

    // Set to false as by default it uses 64 bit, needs further investigation
    val use128BitTraceId = false

    val samplerType = kafkaConfigValues.getString(ConfigProps.SAMPLER_TYPE)
    val samplerParam = kafkaConfigValues.getNumber(ConfigProps.SAMPLER_PARAM)
    val samplerHostPort = kafkaConfigValues.getString(ConfigProps.SAMPLER_HOST_PORT)

    val sampler = new SamplerConfiguration()
      .withType(samplerType)
      .withParam(samplerParam)
      .withManagerHostPort(samplerHostPort)

    val tracerConfig = new Configuration(service)
      .withTracerTags(tags)
        .withTraceId128Bit(use128BitTraceId)
        .withReporter(reporter)
        .withSampler(sampler)
        .withCodec(null)

    tracer = tracerConfig.getTracer
  }
}