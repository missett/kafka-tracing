package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.TestFixtures._
import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.Config
import io.github.missett.kafkatracing.jaeger.analytics.model.Span
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class SpanProcessorTest extends FlatSpec with Matchers with MockitoSugar with Config {
  it should "insert a span in the span store" in {
    val s = span(0)
    val alg = new TestTraceGraphAlgebra(TinkerGraph.open().traversal())
    val proc = new SpanProcessor(alg)
    val context = mock[ProcessorContext]
    val store = mock[KeyValueStore[String, Span]]
    when(context.getStateStore(TopologyLabels.SpanStoreName)).thenReturn(store)

    proc.init(context)
    proc.process(s.spanId, s)

    verify(store, times(1)).put(s.spanId, s)
  }
}