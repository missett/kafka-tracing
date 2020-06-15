package io.github.missett.kafkatracing.jaeger

import java.util

import com.typesafe.config.{ConfigException, ConfigFactory}
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.{ReporterConfiguration, SamplerConfiguration, SenderConfiguration}
import io.jaegertracing.internal.JaegerTracer
import io.jaegertracing.internal.samplers.{ConstSampler, ProbabilisticSampler, RateLimitingSampler, RemoteControlledSampler}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Config {
  object ConfigProps {
    // required
    val SERVICE_NAME = "jaeger.interceptor.service.name"
    // optional
    val TRACER_TAGS = "jaeger.interceptor.tracer.tags"
    // required, default to false
    val REPORTER_LOG_SPANS = "jaeger.interceptor.reporter.log.spans"
    // required, default to 1000
    val REPORTER_FLUSH_INTERVAL = "jaeger.interceptor.reporter.flush.interval"
    // required, default to 100
    val REPORTER_MAX_QUEUE_SIZE = "jaeger.interceptor.max.queue.size"

    // required, one of 'http' or 'udp', default to udp
    val SENDER_TYPE = "jaeger.interceptor.sender.type"
    // required
    val SENDER_HOST = "jaeger.interceptor.sender.host"
    // required
    val SENDER_PORT = "jaeger.interceptor.sender.port"
    // required only if type = http
    val SENDER_ENDPOINT = "jaeger.interceptor.sender.endpoint"

    // required, one of 'probabilistic', 'const', 'ratelimiting', 'remote', default to 'probabilistic', if 'remote' then require hostport to be set
    val SAMPLER_TYPE = "jaeger.interceptor.sampler.type"
    // required, default to 0.1
    val SAMPLER_PARAM = "jaeger.interceptor.sampler.param"
    // optional, required if sampler type is 'remote'
    val SAMPLER_HOST_PORT = "jaeger.interceptor.sampler.hostport"

    def all: List[String] = List(
      SERVICE_NAME, TRACER_TAGS, REPORTER_LOG_SPANS, REPORTER_FLUSH_INTERVAL, REPORTER_MAX_QUEUE_SIZE,
      SENDER_TYPE, SENDER_HOST, SENDER_PORT, SENDER_ENDPOINT, SAMPLER_TYPE, SAMPLER_PARAM, SAMPLER_HOST_PORT,
    )
  }

  def extractJaegerPropsFromKafkaProps(config: java.util.Map[String, _]): util.Map[String, _] =
    config.asScala.filter { case (key, _) => ConfigProps.all.contains(key) }.asJava

  def getTagMapFromTagConfigString(input: String): util.Map[String, String] = input.split(",")
    .map(_.split("="))
    .filter(_.length == 2)
    .map { case Array(key, value) => (key, value) }
    .toMap
    .asJava

  def getOrNull[A](f: => A): A = Try(f) match {
    case Success(x) => x
    case Failure(_) => null.asInstanceOf[A]
  }

  // We go slightly out of our way to configure a tracer object like this because we want to be able to pass
  // our Jaeger config in to the kafka interceptor configs rather than using environment variables (the kafka
  // configs will be configurable via environment variables anyway).

  def fromConf(config: java.util.Map[String, _]): JaegerTracer = {
    val defaults = ConfigFactory.parseResources("kafka-tracing-defaults.conf")

    val kafkaConfigValues = ConfigFactory
      .parseMap(extractJaegerPropsFromKafkaProps(config))
      .withFallback(defaults)

    val service = kafkaConfigValues.getString(ConfigProps.SERVICE_NAME)
    val tags = getTagMapFromTagConfigString(kafkaConfigValues.getString(ConfigProps.TRACER_TAGS))

    val reporterLogSpans = kafkaConfigValues.getBoolean(ConfigProps.REPORTER_LOG_SPANS)
    val reporterFlushInterval = kafkaConfigValues.getInt(ConfigProps.REPORTER_FLUSH_INTERVAL)
    val reporterMaxQueueSize = kafkaConfigValues.getInt(ConfigProps.REPORTER_MAX_QUEUE_SIZE)

    val senderHost = kafkaConfigValues.getString(ConfigProps.SENDER_HOST)
    val senderPort = kafkaConfigValues.getInt(ConfigProps.SENDER_PORT)
    val senderType = kafkaConfigValues.getString(ConfigProps.SENDER_TYPE)

    val senderEndpoint = if (senderType == "http") {
      s"$senderHost:$senderPort${kafkaConfigValues.getString(ConfigProps.SENDER_ENDPOINT)}"
    } else if (senderType == "udp") {
      null
    } else {
      throw new ConfigException.BadValue("jaeger.interceptor.sender.type", "jaeger.interceptor.sender.type must be one of 'udp' or 'http'")
    }

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

    val knownSamplerTypes = List(ConstSampler.TYPE, ProbabilisticSampler.TYPE, RateLimitingSampler.TYPE, RemoteControlledSampler.TYPE)
    val samplerType = kafkaConfigValues.getString(ConfigProps.SAMPLER_TYPE)
    val samplerParam = kafkaConfigValues.getNumber(ConfigProps.SAMPLER_PARAM)

    if (! knownSamplerTypes.contains(samplerType)) {
      throw new ConfigException.BadValue("jaeger.interceptor.sampler.type", s"jaeger.interceptor.sampler.type must be one of ${knownSamplerTypes.mkString(", ")}")
    }

    val samplerHostPort = if (samplerType == RemoteControlledSampler.TYPE) {
      kafkaConfigValues.getString(ConfigProps.SAMPLER_HOST_PORT)
    } else {
      null
    }

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
