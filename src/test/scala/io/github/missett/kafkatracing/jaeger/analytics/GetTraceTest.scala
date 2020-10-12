package io.github.missett.kafkatracing.jaeger.analytics

import org.scalatest.{FlatSpec, Matchers}

class GetTraceTest extends FlatSpec with Matchers {
  it should "do something" in {
    val trace = GetTrace("aee1caf")
    println(trace)
  }
}