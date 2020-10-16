package io.github.missett.kafkatracing.jaeger.analytics

import com.typesafe.config.{Config => TypesafeConfig}
import pureconfig.generic.ProductHint
import pureconfig.{ConfigFieldMapping, SnakeCase, _}
// do not auto-optimise your imports in intellij in this file, the below import is definitely needed btu inellij says it isn't
import pureconfig.generic.auto._

object PureConfig {

  case class RootConfig(
    application: ApplicationConfig,
    streams: StreamsConfig,
  )

  implicit def applicationconfig[T <: SnakeCasedConfig]: ProductHint[T] = ProductHint(ConfigFieldMapping(SnakeCase, SnakeCase))

  trait SnakeCasedConfig

  case class ApplicationConfig(
    topics: Topics,
  ) extends SnakeCasedConfig

  case class InputTopics(
    spans: String
  ) extends SnakeCasedConfig

  case class Topics(
    inputs: InputTopics,
  ) extends SnakeCasedConfig

  implicit val streamsconfig: ConfigReader[StreamsConfig] = ConfigReader[TypesafeConfig].map { config =>
    val props = new StreamsConfig()
    config.entrySet().forEach { entry => props.put(entry.getKey, entry.getValue.unwrapped()); () }
    props
  }

  type StreamsConfig = java.util.Properties

  trait Config {
    val config: RootConfig = ConfigSource.default.loadOrThrow[RootConfig]
  }

}
