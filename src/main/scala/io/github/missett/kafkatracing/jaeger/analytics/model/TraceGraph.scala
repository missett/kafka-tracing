package io.github.missett.kafkatracing.jaeger.analytics.model

import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.RefType.Type
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

object TraceGraph {
  trait TraceGraphAlgebra {
    def create(store: StoreAccess[String, Span], span: Span): GraphTraversalSource
  }

  class TraceGraphAlgebraImp extends TraceGraphAlgebra {
    private def vertex(span: Span, g: GraphTraversalSource) = g.addV("span")
      .property("span-id", span.spanId)
      .property("operation", span.operationName)
      .property("duration", span.duration)
      .property("start-time", span.startTime)
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
  }
}