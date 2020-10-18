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

class InfiniteRange extends Iterator[Int] {
  var i: Int = -1
  override def hasNext: Boolean = true
  override def next(): Int = { i += 1; i }
}

class SpanProcessorTest extends FlatSpec with Matchers with OptionValues with Config {
  behavior of "SpanLogic"

  val range = new InfiniteRange

  def getSpan(index: Int, parents: List[Span] = List.empty) = {
    val traceid = "traceid"
    val spanid = s"span-$index"
    val operation = "operation"

    Span(traceid, spanid, operation, Some(parents.map(s => Reference(s.traceId, s.spanId, RefType.FOLLOWS_FROM))), 1, s"$index", s"$index", List.empty, Process("service-name", List.empty))
  }

  it should "insert a span in the span store" in {
    val span = getSpan(0)
    val spans = new TestSpanStore
    val logic = new SpanLogic(spans)
    logic.process(span)
    spans.store.get(span.spanId).value should equal (span)
  }

  it should "return the span and a parent span when processing" in {
    val spans = new TestSpanStore
    val logic = new SpanLogic(spans)

    val parent = getSpan(0)
    val child = getSpan(1, List(parent))

    logic.process(parent) should equal (List(parent))
    logic.process(child) should equal (List(child, parent))
  }

  it should "return a chained sequence of spans" in {
    val spans = new TestSpanStore
    val logic = new SpanLogic(spans)
    val a = getSpan(0)
    val b = getSpan(1, List(a))
    val c = getSpan(2, List(b))

    val result = List(a, b, c).foldLeft(List.empty[Span])((_, curr) => {
      logic.process(curr)
    })

    result.size should equal (3)
  }

  it should "return a set of spans with branching relationships" in {
    val spans = new TestSpanStore
    val logic = new SpanLogic(spans)
    val a = getSpan(0)
    val b = getSpan(1)
    val c = getSpan(2, List(a, b))

    val result = List(a, b, c).foldLeft(List.empty[Span])((_, curr) => {
      logic.process(curr)
    })

    result.size should equal (3)
  }
}