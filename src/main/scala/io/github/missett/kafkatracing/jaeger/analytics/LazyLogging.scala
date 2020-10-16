package io.github.missett.kafkatracing.jaeger.analytics

import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}