package org.missett.kafka.interceptors.jaeger

import io.opentracing.propagation.Format
import org.scalatest.{FlatSpec, Matchers}

class ContextHeaderEncoderTest extends FlatSpec with Matchers {
  behavior of "ContextHeaderEncoder"

  it should "return null when given some empty headers" in new JaegerInterceptorTesting {
    val rec = record(key = 1, value = "one")
    val coder = new ContextHeaderEncoder(rec.headers())
    val (_, _, tracer) = getTestComponents

    val result = tracer.extract(Format.Builtin.TEXT_MAP, coder)

    result should equal (null)
  }

  it should "return null when given some headers containing data not related to Jaeger tracing" in new JaegerInterceptorTesting {
    val rec = record(key = 1, value = "one")
    rec.headers().add("foo", "bar".getBytes)
    val coder = new ContextHeaderEncoder(rec.headers())
    val (_, _, tracer) = getTestComponents

    val result = tracer.extract(Format.Builtin.TEXT_MAP, coder)

    result should equal (null)
  }
}