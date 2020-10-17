package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.Config
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model._
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.collection.mutable

class TestSpanStore extends StoreAccess[String, Span] {
  val store = mutable.Map.empty[String, Span]
  override def get(key: String): Option[Span] = store.get(key)
  override def set(key: String, value: Span): Span = { store.put(key, value); value }
}

class SpanProcessorTest extends FlatSpec with Matchers with OptionValues with Config {
  behavior of "SpanLogic"

  it should "insert a span with no references in the trace root store and in the span store" in {
    val span = Span("traceid", "spanid", "operation", None, 1, "datetime", "duration", List.empty, Process("service", List.empty))
    val roots = new TestSpanStore
    val spans = new TestSpanStore
    val logic = new SpanLogic(roots, spans)
    logic.process(span)
    roots.store.get(span.traceId).value should equal (span)
    spans.store.get(span.spanId).value should equal (span)
  }

  it should "not insert a span with references in the trace root store, but insert it in the span store" in {
    val span = Span("traceid", "spanid", "operation", Some(List.empty), 1, "datetime", "duration", List.empty, Process("service", List.empty))
    val roots = new TestSpanStore
    val spans = new TestSpanStore
    val logic = new SpanLogic(roots, spans)
    logic.process(span)
    roots.store.get(span.traceId) should equal (None)
    spans.store.get(span.spanId).value should equal (span)
  }
}