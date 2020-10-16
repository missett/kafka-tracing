package io.github.missett.kafkatracing.jaeger.analytics.streams

import cats.effect.{Async, Resource}
import io.github.missett.kafkatracing.jaeger.analytics.PureConfig.RootConfig
import io.github.missett.kafkatracing.jaeger.analytics.serialization.ApplicationSerdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}

import scala.util.{Failure, Success, Try}

object Streams {
  def resource[F[_]](config: RootConfig, serdes: ApplicationSerdes)(implicit F: Async[F]): Resource[F, KafkaStreams] = {
    Resource.make[F, KafkaStreams](F.async[KafkaStreams](cb => {
      val builder = new StreamsBuilder()
      val topology = builder.build(config.streams)

      ApplicationTopology.build(topology, serdes, config)

      val streams = new KafkaStreams(builder.build(), config.streams)

      streams.setUncaughtExceptionHandler((_: Thread, e: Throwable) => {
        cb(Left(e))
      })

      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        cb(Right(streams))
      }))

      Try(streams.start()) match {
        case Failure(e) => cb(Left(e))
        case Success(_) => ()
      }
    }))(
      s => F.pure {
        s.close()
      }
    )
  }
}
