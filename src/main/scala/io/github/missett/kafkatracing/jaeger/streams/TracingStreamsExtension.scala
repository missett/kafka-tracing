package io.github.missett.kafkatracing.jaeger.streams

import io.jaegertracing.internal.JaegerTracer
import org.apache.kafka.streams.scala.kstream.KStream

object TracingStreamsExtension {
  implicit class TracingStreamsExtensionImp[K, V](streams: KStream[K, V]) {
    def trace(operation: String, f: KStream[K, V] => KStream[K, V])(implicit tracer: JaegerTracer): KStream[K, V] = {
      f(
        streams.transform(new StartTraceTransformSupplier[K, V](operation))
      )
        .transform(new EndTraceTransformSupplier[K, V](operation))
    }
  }
}
