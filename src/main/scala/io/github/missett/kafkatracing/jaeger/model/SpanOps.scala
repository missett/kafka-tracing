package io.github.missett.kafkatracing.jaeger.model

import io.jaegertracing.internal.JaegerTracer
import io.opentracing
import io.opentracing.Tracer.SpanBuilder
import io.opentracing.propagation.{Format, TextMap}
import io.opentracing.{References, SpanContext, Tracer}

trait ContextCoderOps[A] {
  def injector(carrier: A): TextMap

  def extractor(carrier: A): TextMap

  def extract(tracer: Tracer, carrier: A): Option[SpanContext] =
    Option(tracer.extract(Format.Builtin.TEXT_MAP, extractor(carrier)))

  def inject(tracer: Tracer, carrier: A, context: SpanContext) =
    tracer.inject(context, Format.Builtin.TEXT_MAP, injector(carrier))

  def follows(tracer: Tracer, builder: SpanBuilder, carrier: A): Option[SpanBuilder] =
    extract(tracer, carrier).map(context => builder.addReference(References.FOLLOWS_FROM, context))
}

sealed trait SpanReference
case object NoReference extends SpanReference
case object FollowsFrom extends SpanReference

trait Span[C] {
  def operation: String
  def tags: List[(String, String)]
  def context: C
  def ref: SpanReference

  protected def builder(implicit tracer: JaegerTracer, ops: ContextCoderOps[C]): SpanBuilder = {
    val b = tracer.buildSpan(operation)

    tags.foreach {
      case (key, value) => b.withTag(key, value)
    }

    ref match {
      case NoReference => ()
      case FollowsFrom => ops.follows(tracer, b, context)
    }

    b
  }

  def start(implicit tracer: JaegerTracer, ops: ContextCoderOps[C]): opentracing.Span = {
    val span = builder.start()
    ops.inject(tracer, context, span.context())
    span
  }

  def instant(implicit tracer: JaegerTracer, ops: ContextCoderOps[C]) =
    start.finish()
}
