package io.github.missett.kafkatracing.jaeger.analytics.streams

import java.time.Duration

import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.RootConfig
import io.github.missett.kafkatracing.jaeger.analytics.serialization.ApplicationSerdes
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder

import scala.collection.JavaConverters._

object TopologyLabels {
  val SpanSourceName = "span-source"
  val SpanProcessorName = "span-processor"
  val SpanStoreName = "span-store"
}

object ApplicationTopology {
  private val retention = Duration.ofDays(7)

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

    topology.addStateStore(
      new KeyValueStoreBuilder(Stores.inMemoryKeyValueStore(TopologyLabels.SpanStoreName), serdes.StringSerde, serdes.SpanSerde, new SystemTime)
        .withLoggingEnabled(Map("retention.ms" -> s"${retention.toMillis}", "cleanup.policy" -> "compact,delete").asJava),
      TopologyLabels.SpanProcessorName
    )
  }
}