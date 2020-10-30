package io.github.missett.kafkatracing.jaeger.analytics.model

import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.RefType.Type
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversal, GraphTraversalSource}
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

import scala.util.Try

object TraceGraph {
  type TraversalNode = java.util.LinkedHashMap[String, Vertex]
  type Traversal = GraphTraversal[Vertex, TraversalNode]

  trait TraceGraphAlgebra {
    def create(store: StoreAccess[String, Span], span: Span): GraphTraversalSource

    def execute(engine: GremlinGroovyScriptEngine, g: GraphTraversalSource, traversal: String): Either[Throwable, Traversal]
  }

  class TraceGraphAlgebraImp extends TraceGraphAlgebra {
    private def vertex(span: Span, g: GraphTraversalSource) = g.addV("span")
      .property("spanId", span.spanId)
      .property("operationName", span.operationName)
      .property("duration", span.duration)
      .property("startTime", span.startTime)
      .property("serviceName", span.process.serviceName)
      .next()

    private def refsToSpans(store: StoreAccess[String, Span], refs: List[Reference]): List[(Type, Span)] = refs
      .map(r => (r.refType, r.spanId))
      .flatMap(r => store.get(r._2) match {
        case Some(span) => Some((r._1, span))
        case None       => None
      })

    private def traverse(store: StoreAccess[String, Span], span: Span, g: GraphTraversalSource): Vertex = {
      val v = vertex(span, g)
      val refs = span.references.getOrElse(List.empty[Reference])

      refsToSpans(store, refs).foreach {
        case (ref, span) => g
          .addE(ref.toString)
          .from(v)
          .to(traverse(store, span, g))
          .iterate()
      }

      v
    }

    override def create(store: StoreAccess[String, Span], span: Span): GraphTraversalSource = {
      val graph = TinkerGraph.open()
      val g = graph.traversal()
      traverse(store, span, g)
      g
    }

    override def execute(engine: GremlinGroovyScriptEngine, g: GraphTraversalSource, traversal: String): Either[Throwable, Traversal] = {
      val bindings = engine.createBindings()
      bindings.put("g", g)
      Try(engine.eval(traversal, bindings).asInstanceOf[Traversal]).toEither
    }
  }
}