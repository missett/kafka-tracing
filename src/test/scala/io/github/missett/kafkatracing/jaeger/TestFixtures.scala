package io.github.missett.kafkatracing.jaeger

import java.text.SimpleDateFormat
import java.util.Date

import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.TraceGraph.{TraceGraphAlgebra, Traversal}
import io.github.missett.kafkatracing.jaeger.analytics.model.{Process, RefType, Reference, Span}
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

import scala.collection.mutable

object TestFixtures {
  def now: Long = System.currentTimeMillis()

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

  class TestSpanStore(var store: mutable.Map[String, Span] = mutable.Map.empty[String, Span]) extends StoreAccess[String, Span] {
    override def get(key: String): Option[Span] = store.get(key)
    override def set(key: String, value: Span): Span = { store.put(key, value); value }
  }

  class TestTraceGraphAlgebra(g: GraphTraversalSource, t: Either[Throwable, Traversal]) extends TraceGraphAlgebra {
    override def create(store: StoreAccess[String, Span], span: Span): GraphTraversalSource = g

    override def execute(engine: GremlinGroovyScriptEngine, g: GraphTraversalSource, traversal: String): Either[Throwable, Traversal] = t
  }
}

