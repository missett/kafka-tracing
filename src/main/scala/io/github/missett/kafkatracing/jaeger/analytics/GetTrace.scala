package io.github.missett.kafkatracing.jaeger.analytics

import java.util

import io.grpc.netty.NettyChannelBuilder
import io.jaegertracing.analytics.model.{Converter, Trace}
import io.jaegertracing.api_v2.Model.Span
import io.jaegertracing.api_v2.Query.GetTraceRequest
import io.jaegertracing.api_v2.QueryServiceGrpc

object GetTrace {
  def apply(traceid: String): Trace = {
    val channel = NettyChannelBuilder.forTarget("localhost:16686").usePlaintext().build()
    val service = QueryServiceGrpc.newBlockingStub(channel)

    val response = service.getTrace(
      GetTraceRequest.newBuilder().setTraceId(Converter.toProtoId(traceid)).build()
    )

    val spans = new util.ArrayList[Span]()

    while (response.hasNext) {
      spans.addAll(response.next().getSpansList)
    }

    val trace = Converter.toModel(spans)

    trace
  }
}