package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.Span
import io.github.missett.kafkatracing.jaeger.analytics.model.TraceGraph.{TraceGraphAlgebra, TraceGraphAlgebraImp}
import io.github.missett.kafkatracing.jaeger.analytics.serialization.KafkaStoreAccess
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore

class SpanProcessor(tracegraph: TraceGraphAlgebra) extends Processor[String, Span] with LazyLogging {
  var context: ProcessorContext = _

  val store = new KafkaStoreAccess[String, Span](this.context.getStateStore(TopologyLabels.SpanStoreName).asInstanceOf[KeyValueStore[String, Span]])

  override def init(context: ProcessorContext): Unit = { this.context = context; () }

  override def process(key: String, span: Span): Unit = {
    logger.info(s"recieved span [$key] -> [$span]")

    store.set(key, span)

    tracegraph.create(store, span)

    ()
  }

  override def close(): Unit = ()
}

class SpanProcessorSupplier extends ProcessorSupplier[String, Span] {
  override def get(): Processor[String, Span] = new SpanProcessor(new TraceGraphAlgebraImp)
}