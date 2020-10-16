package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.RootConfig
import io.github.missett.kafkatracing.jaeger.analytics.serialization.ApplicationSerdes
import org.apache.kafka.streams.Topology

object TopologyLabels {
  val SpanSourceName = "span-source"
  val SpanProcessorName = "span-processor"
}

object ApplicationTopology {
  def build(topology: Topology, serdes: ApplicationSerdes, config: RootConfig): Topology = {
    topology.addSource(
      TopologyLabels.SpanSourceName,
      serdes.StringSerde.deserializer(),
      serdes.SpanSerde.deserializer(),
      config.application.topics.inputs.spans
    )

    topology.addProcessor(
      TopologyLabels.SpanProcessorName,
      new SpanProcessorSupplier,
      TopologyLabels.SpanSourceName
    )
  }
}