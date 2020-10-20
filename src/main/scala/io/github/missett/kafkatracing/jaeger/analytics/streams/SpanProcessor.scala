package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.{Reference, Span}
import io.github.missett.kafkatracing.jaeger.analytics.serialization.KafkaStoreAccess
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

class SpanLogic(store: StoreAccess[String, Span]) {
  private def vertex(span: Span, g: GraphTraversalSource) =
    g.addV("span")
      .property("span-id", span.spanId)
      .property("operation", span.operationName)
      .property("duration", span.duration)
      .property("start-time", span.startTime)
      .next()

  private def traverse(span: Span, v: Vertex, g: GraphTraversalSource): List[Span] = {
    val refs = span.references.getOrElse(List.empty[Reference])
    val spans = refs.map(_.spanId).flatMap(store.get)

    span :: spans.flatMap(s => {
      val x = vertex(s, g)
      g.addE("follows-from").from(v).to(x).iterate()
      traverse(s, x ,g)
    })
  }

  def process(span: Span): (List[Span], GraphTraversalSource) = {
    store.set(span.spanId, span)
    val graph = TinkerGraph.open()
    val g = graph.traversal()
    val spans = traverse(span, vertex(span, g), g)
    (spans, g)
  }
}

class SpanProcessor extends Processor[String, Span] with LazyLogging {
  var context: ProcessorContext = _

  val spanstore = new KafkaStoreAccess[String, Span](this.context.getStateStore(TopologyLabels.SpanStoreName).asInstanceOf[KeyValueStore[String, Span]])
  val logic = new SpanLogic(spanstore)

  override def init(context: ProcessorContext): Unit = { this.context = context; () }

  override def process(key: String, value: Span): Unit = {
    logger.info(s"recieved span [$key] -> [$value]")
    logic.process(value)
    ()
  }

  override def close(): Unit = ()
}

class SpanProcessorSupplier extends ProcessorSupplier[String, Span] {
  override def get(): Processor[String, Span] = new SpanProcessor
}