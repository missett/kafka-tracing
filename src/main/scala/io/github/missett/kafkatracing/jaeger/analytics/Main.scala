package io.github.missett.kafkatracing.jaeger.analytics

import cats.effect.{ExitCode, IO, IOApp, Resource}
import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.Config
import io.github.missett.kafkatracing.jaeger.analytics.serialization.ApplicationSerdes
import io.github.missett.kafkatracing.jaeger.analytics.streams.Streams
import org.apache.kafka.streams.KafkaStreams

object Main extends IOApp with Config {
  def resources: Resource[IO, KafkaStreams] = for {
    streams <- Streams.resource[IO](config, new ApplicationSerdes {})
  } yield streams

  override def run(args: List[String]): IO[ExitCode] = resources.use(_ => for {
    exit <- IO(ExitCode.Success)
  } yield exit)
}