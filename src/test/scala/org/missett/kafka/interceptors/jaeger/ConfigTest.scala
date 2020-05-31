package org.missett.kafka.interceptors.jaeger

import java.util

import Config.ConfigProps._
import org.scalatest.{FlatSpec, Matchers}

class ConfigTest extends FlatSpec with Matchers {
  behavior of "Configuration"

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
}