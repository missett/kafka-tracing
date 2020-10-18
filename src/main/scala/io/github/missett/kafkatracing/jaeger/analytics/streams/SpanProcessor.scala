package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.{Reference, Span}
import io.github.missett.kafkatracing.jaeger.analytics.serialization.KafkaStoreAccess
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore

class SpanLogic(store: StoreAccess[String, Span]) {
  private def traverse(span: Span): List[Span] = {
    val refs = span.references.getOrElse(List.empty[Reference])
    val spans = refs.map(_.spanId).flatMap(store.get)
    span :: spans.flatMap(traverse)
  }

  def process(span: Span): List[Span] = {
    store.set(span.spanId, span)
    traverse(span)
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