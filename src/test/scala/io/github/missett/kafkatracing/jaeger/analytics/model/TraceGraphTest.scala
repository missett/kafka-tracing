package io.github.missett.kafkatracing.jaeger.analytics.model

import io.github.missett.kafkatracing.jaeger.TestFixtures._
import io.github.missett.kafkatracing.jaeger.analytics.model.TraceGraph.TraceGraphAlgebraImp
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable

class TraceGraphTest extends FlatSpec with Matchers with EitherValues {
  behavior of "TraceGraphImp"

  it should "return a chained sequence of spans as a graph" in {
    val a = span(0)
    val b = span(1, List(a))
    val c = span(2, List(b))
    val store = new TestSpanStore(mutable.Map[String, Span]((a.spanId, a), (b.spanId, b), (c.spanId, c)))
    val alg = new TraceGraphAlgebraImp()
    val g = alg.create(store, c)

    val search = g.V()
      .has("span", "spanId", c.spanId)
      .as("c")
      .out("FOLLOWS_FROM")
      .has("span", "spanId", b.spanId)
      .as("b")
      .out("FOLLOWS_FROM")
      .has("span", "spanId", a.spanId)
      .as("a")
      .select("c", "b", "a")

    val vertices = search.toList.get(0).asInstanceOf[java.util.LinkedHashMap[String, Vertex]]

    vertices.get("c").property[String]("duration").value() should equal (c.duration)
    vertices.get("c").property[String]("startTime").value() should equal (c.startTime)
    vertices.get("b").property[String]("duration").value() should equal (b.duration)
    vertices.get("b").property[String]("startTime").value() should equal (b.startTime)
    vertices.get("a").property[String]("duration").value() should equal (a.duration)
    vertices.get("a").property[String]("startTime").value() should equal (a.startTime)
  }

  it should "what happens when a span refers to a non existant span as a parent" in {
    val a = span(0)
    val b = span(1, List(a))
    val store = new TestSpanStore(mutable.Map[String, Span]((b.spanId, b)))
    val alg = new TraceGraphAlgebraImp()

    alg.create(store, b)
  }

  it should "return a set of spans with branching relationships as a graph" in {
    val a = span(0)
    val b = span(1)
    val c = span(2, List(a, b))
    val store = new TestSpanStore(mutable.Map[String, Span]((a.spanId, a), (b.spanId, b), (c.spanId, c)))
    val alg = new TraceGraphAlgebraImp()
    val g = alg.create(store, c)

    {
      val search = g.V()
        .has("span", "spanId", c.spanId)
        .as("c")
        .out("FOLLOWS_FROM")
        .has("span", "spanId", b.spanId)
        .as("b")
        .select("c", "b")

      val vertices = search.toList.get(0).asInstanceOf[java.util.LinkedHashMap[String, Vertex]]

      vertices.get("c").property[String]("duration").value() should equal (c.duration)
      vertices.get("c").property[String]("startTime").value() should equal (c.startTime)
      vertices.get("b").property[String]("duration").value() should equal (b.duration)
      vertices.get("b").property[String]("startTime").value() should equal (b.startTime)
    }

    {
      val search = g.V()
        .has("span", "spanId", c.spanId)
        .as("c")
        .out("FOLLOWS_FROM")
        .has("span", "spanId", a.spanId)
        .as("a")
        .select("c", "a")

      val vertices = search.toList.get(0).asInstanceOf[java.util.LinkedHashMap[String, Vertex]]

      vertices.get("c").property[String]("duration").value() should equal (c.duration)
      vertices.get("c").property[String]("startTime").value() should equal (c.startTime)
      vertices.get("a").property[String]("duration").value() should equal (a.duration)
      vertices.get("a").property[String]("startTime").value() should equal (a.startTime)
    }
  }

  it should "return a traversal when given a string representing a query in .execute" in {
    val a = span(0)
    val b = span(1)
    val c = span(2, List(a, b))
    val store = new TestSpanStore(mutable.Map[String, Span]((a.spanId, a), (b.spanId, b), (c.spanId, c)))
    val alg = new TraceGraphAlgebraImp()
    val g = alg.create(store, c)
    val engine = new GremlinGroovyScriptEngine()
    val traversal =
      s"""g.V()
         |.has("span", "spanId", "${c.spanId}")
         |.as("end")
         |.out("FOLLOWS_FROM")
         |.has("span", "spanId", "${a.spanId}")
         |.as("start")
         |.select("start", "end")""".stripMargin

    val result = alg.execute(engine, g, traversal).right.value.toList

    val iter = result.iterator().asScala
    val pairs = iter.map(n => (n.get("start"), n.get("end")))

    pairs.foreach { case (start, end) =>
      start.property[String]("startTime").value() should equal (a.startTime)
      end.property[String]("startTime").value() should equal (c.startTime)
    }
  }
}