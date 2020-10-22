package io.github.missett.kafkatracing.jaeger.analytics.streams

import java.text.SimpleDateFormat
import java.util.Date

import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.Config
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model._
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.collection.mutable

class TestSpanStore extends StoreAccess[String, Span] {
  val store = mutable.Map.empty[String, Span]
  override def get(key: String): Option[Span] = store.get(key)
  override def set(key: String, value: Span): Span = { store.put(key, value); value }
}

class SpanProcessorTest extends FlatSpec with Matchers with OptionValues with Config {
  behavior of "SpanLogic"

  val now = System.currentTimeMillis()

  def span(index: Int, parents: List[Span] = List.empty) = {
    val traceid = "traceid"
    val spanid = s"span-$index"
    val operation = "operation"
    val duration = 9000L
    val start = index * (duration + 10)
    val starttime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date(start))
    val durationtime = s"${duration / 1000}s"

    Span(traceid, spanid, operation, Some(parents.map(s => Reference(s.traceId, s.spanId, RefType.FOLLOWS_FROM))), 1, starttime, durationtime, List.empty, Process("service-name", List.empty))
  }

  it should "insert a span in the span store" in {
    val a = span(0)
    val store = new TestSpanStore
    val logic = new SpanLogic(store)
    logic.process(a)
    store.get(a.spanId).value should equal (a)
  }

  it should "return a chained sequence of spans as a graph" in {
    val store = new TestSpanStore
    val logic = new SpanLogic(store)
    val a = span(0)
    val b = span(1, List(a))
    val c = span(2, List(b))

    val results = List(a, b, c).map(curr => {
      logic.process(curr)
    })

    val g = results.last

    val search = g.V()
      .has("span", "span-id", c.spanId)
      .as("c")
      .out("FOLLOWS_FROM")
      .has("span", "span-id", b.spanId)
      .as("b")
      .out("FOLLOWS_FROM")
      .has("span", "span-id", a.spanId)
      .as("a")
      .select("c", "b", "a")

    val vertices = search.toList.get(0).asInstanceOf[java.util.LinkedHashMap[String, Vertex]]

    vertices.get("c").property[String]("duration").value() should equal (c.duration)
    vertices.get("c").property[String]("start-time").value() should equal (c.startTime)
    vertices.get("b").property[String]("duration").value() should equal (b.duration)
    vertices.get("b").property[String]("start-time").value() should equal (b.startTime)
    vertices.get("a").property[String]("duration").value() should equal (a.duration)
    vertices.get("a").property[String]("start-time").value() should equal (a.startTime)
  }

  it should "return a set of spans with branching relationships as a graph" in {
    val store = new TestSpanStore
    val logic = new SpanLogic(store)
    val a = span(0)
    val b = span(1)
    val c = span(2, List(a, b))

    val results = List(a, b, c).map(curr => {
      logic.process(curr)
    })

    val g = results.last

    {
      val search = g.V()
        .has("span", "span-id", c.spanId)
        .as("c")
        .out("FOLLOWS_FROM")
        .has("span", "span-id", b.spanId)
        .as("b")
        .select("c", "b")

      val vertices = search.toList.get(0).asInstanceOf[java.util.LinkedHashMap[String, Vertex]]

      vertices.get("c").property[String]("duration").value() should equal (c.duration)
      vertices.get("c").property[String]("start-time").value() should equal (c.startTime)
      vertices.get("b").property[String]("duration").value() should equal (b.duration)
      vertices.get("b").property[String]("start-time").value() should equal (b.startTime)
    }

    {
      val search = g.V()
        .has("span", "span-id", c.spanId)
        .as("c")
        .out("FOLLOWS_FROM")
        .has("span", "span-id", a.spanId)
        .as("a")
        .select("c", "a")

      val vertices = search.toList.get(0).asInstanceOf[java.util.LinkedHashMap[String, Vertex]]

      vertices.get("c").property[String]("duration").value() should equal (c.duration)
      vertices.get("c").property[String]("start-time").value() should equal (c.startTime)
      vertices.get("a").property[String]("duration").value() should equal (a.duration)
      vertices.get("a").property[String]("start-time").value() should equal (a.startTime)
    }
  }
}