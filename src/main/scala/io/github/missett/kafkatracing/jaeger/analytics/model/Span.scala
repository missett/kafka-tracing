package io.github.missett.kafkatracing.jaeger.analytics.model

object RefType extends Enumeration {
  type Type = Value
  val FOLLOWS_FROM, CHILD_OF: Type = Value
}

case class Tag(key: String, vStr: Option[String])

case class Process(
  serviceName: String,
  tags: List[Tag]
)

case class Reference(
  traceId: String,
  spanId: String,
  refType: RefType.Type,
)

case class Span(
  traceId: String,
  spanId: String,
  operationName: String,
  references: Option[List[Reference]],
  flags: Int,
  startTime: String,
  duration: String,
  tags: List[Tag],
  process: Process
)