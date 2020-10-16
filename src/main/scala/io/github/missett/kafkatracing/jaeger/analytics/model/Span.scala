package io.github.missett.kafkatracing.jaeger.analytics.model

sealed trait RefType
case object FOLLOWS_FROM extends RefType
case object CHILD_OF extends RefType

case class Tag(key: String, vStr: String)

case class Process(
  serviceName: String,
  tags: List[Tag]
)

case class Reference(
  traceId: String,
  spanId: String,
  refType: Option[List[RefType]],
  flags: Int,
  startTime: Long, // parsed from ISO timestamp
  duration: Long, // parse from a decimal float in seconds
  tags: List[Tag],
  process: Process
)

case class Span(
  traceId: String,
  spanId: String,
  operationName: String,
  references: Option[List[Map[String, String]]]
)