package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.DataAccess.StoreAccess
import io.github.missett.kafkatracing.jaeger.analytics.model.Span
import io.github.missett.kafkatracing.jaeger.analytics.serialization.KafkaStoreAccess
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore

class SpanLogic(roots: StoreAccess[String, Span], spans: StoreAccess[String, Span]) {
  private def root(span: Span): Span = {
    roots.set(span.traceId, span)
    spans.set(span.spanId, span)
  }

  private def nonroot(span: Span): Span = {
    spans.set(span.spanId, span)
  }

  def process(span: Span): Span = span.references match {
    case Some(_) => nonroot(span)
    case None    => root(span)
  }
}

class SpanProcessor extends Processor[String, Span] with LazyLogging {
  var context: ProcessorContext = _

  val rootstore = new KafkaStoreAccess[String, Span](this.context.getStateStore(TopologyLabels.TraceRootSpanStoreName).asInstanceOf[KeyValueStore[String, Span]])
  val spanstore = new KafkaStoreAccess[String, Span](this.context.getStateStore(TopologyLabels.SpanStoreName).asInstanceOf[KeyValueStore[String, Span]])
  val logic = new SpanLogic(rootstore, spanstore)

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