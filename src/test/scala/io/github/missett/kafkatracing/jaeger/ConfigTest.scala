package io.github.missett.kafkatracing.jaeger

import com.typesafe.config.ConfigException
import io.github.missett.kafkatracing.jaeger.Config.ConfigProps
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ConfigTest extends FlatSpec with Matchers {

  behavior of "Config.getTagMapFromConfigString"

  it should "convert a string of tags to a tag map" in {
    val op = Config.getTagMapFromTagConfigString("foo=bar,bar=baz,baz=foo")
    op should equal (Map("foo" -> "bar", "bar" -> "baz", "baz" -> "foo").asJava)
  }

  it should "return an empty map when given an empty string" in {
    Config.getTagMapFromTagConfigString("") should equal (Map.empty[String, String].asJava)
  }

  behavior of "Config.fromConf"

  it should "throw an exception if the service name is missing" in {
    a [ConfigException.Missing] should be thrownBy { Config.fromConf(Map.empty[String, String].asJava) }
  }

  it should "default tracer to use only default tags if no tag config is given" in {
    val tracer = Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "udp",
      ConfigProps.SENDER_HOST -> "localhost",
      ConfigProps.SENDER_PORT -> "9999"
    ).asJava)

    tracer.tags().size() should equal (3)
    tracer.tags().get("hostname").asInstanceOf[String] should not be null
    tracer.tags().get("jaeger.version").asInstanceOf[String] should not be null
    tracer.tags().get("ip").asInstanceOf[String] should not be null
  }

  it should "require both sender.host and sender.port to be set if sender.type is udp" in {
    a [ConfigException.Missing] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "udp",
      ConfigProps.SENDER_HOST -> "localhost"
    ).asJava) }

    a [ConfigException.Missing] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "udp",
      ConfigProps.SENDER_PORT -> "9999"
    ).asJava) }
  }

  it should "require sender.host and sender.port to be set if sender.type is http" in {
    a [ConfigException.Missing] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "http",
      ConfigProps.SENDER_HOST -> "http://localhost",
      ConfigProps.SENDER_ENDPOINT -> "/endpoint"
    ).asJava) }

    a [ConfigException.Missing] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "http",
      ConfigProps.SENDER_PORT -> "9999",
      ConfigProps.SENDER_ENDPOINT -> "/endpoint"
    ).asJava) }
  }

  it should "throw an error if sender.type is not one of http or udp" in {
    a [ConfigException.BadValue] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "foo",
      ConfigProps.SENDER_HOST -> "localhost",
      ConfigProps.SENDER_PORT -> "9999"
    ).asJava) }
  }

  it should "return a tracer configured with a udp sender when given a udp config" in {
    val tracer = Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "udp",
      ConfigProps.SENDER_HOST -> "localhost",
      ConfigProps.SENDER_PORT -> "9999"
    ).asJava)

    tracer.toString should include regex ".+sender=UdpSender.+"
  }

  it should "return a tracer configured with an http sender when given an http config" in {
    val tracer = Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "http",
      ConfigProps.SENDER_HOST -> "http://localhost",
      ConfigProps.SENDER_PORT -> "9999",
      ConfigProps.SENDER_ENDPOINT -> "/endpoint"
    ).asJava)

    tracer.toString should include regex ".+sender=HttpSender.+"
  }

  it should "throw an exception if the sampler is an unknown type" in {
    a [ConfigException.BadValue] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "udp",
      ConfigProps.SENDER_HOST -> "localhost",
      ConfigProps.SENDER_PORT -> "9999",
      ConfigProps.SAMPLER_TYPE -> "unknown"
    ).asJava) }
  }

  it should "throw an exception if the sampler type is remote and sampler host port is null" in {
    a [ConfigException.Missing] should be thrownBy { Config.fromConf(Map(
      ConfigProps.SERVICE_NAME -> "service-name",
      ConfigProps.SENDER_TYPE -> "udp",
      ConfigProps.SENDER_HOST -> "localhost",
      ConfigProps.SENDER_PORT -> "9999",
      ConfigProps.SAMPLER_TYPE -> "remote"
    ).asJava) }
  }

}