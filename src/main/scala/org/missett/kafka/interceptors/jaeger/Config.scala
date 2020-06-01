package org.missett.kafka.interceptors.jaeger

import java.util

import com.typesafe.config.ConfigFactory
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.{ReporterConfiguration, SamplerConfiguration, SenderConfiguration}
import io.jaegertracing.internal.JaegerTracer

import scala.collection.JavaConverters._

object Config {
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

    def all: List[String] = List(
      SERVICE_NAME, TRACER_TAGS, REPORTER_LOG_SPANS, REPORTER_FLUSH_INTERVAL, REPORTER_MAX_QUEUE_SIZE,
      SENDER_HOST, SENDER_PORT, SENDER_ENDPOINT, SAMPLER_TYPE, SAMPLER_PARAM, SAMPLER_HOST_PORT,
    )

    def extractJaegerPropsFromKafkaProps(config: java.util.Map[String, _]): util.Map[String, _] =
      config.asScala.filter { case (key, _) => ConfigProps.all.contains(key) }.asJava
  }

  // We go slightly out of our way to configure a tracer object like this because we want to be able to pass
  // our Jaeger config in to the kafka interceptor configs rather than using environment variables (the kafka
  // configs will be configurable via environment variables anyway).

  def fromConf(config: java.util.Map[String, _]): JaegerTracer = {
    val kafkaConfigValues = ConfigFactory.parseMap(ConfigProps.extractJaegerPropsFromKafkaProps(config))

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

    tracerConfig.getTracer
  }
}
