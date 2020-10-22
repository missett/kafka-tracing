package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.RefType.Type
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

  private def traverse(span: Span, g: GraphTraversalSource): Vertex = {
    val refs = span.references.getOrElse(List.empty[Reference])

    val relations: List[(Type, Span)] = refs.map(r => (r.refType, r.spanId)).flatMap(r => store.get(r._2) match {
      case Some(span) => Some((r._1, span))
      case None       => None
    })

    val v = vertex(span, g)

    relations.foreach {
      case (ref, span) => g.addE(ref.toString).from(v).to(traverse(span, g)).iterate()
    }

    v
  }

  def process(span: Span): GraphTraversalSource = {
    store.set(span.spanId, span)
    val graph = TinkerGraph.open()
    val g = graph.traversal()
    traverse(span, g)
    g
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