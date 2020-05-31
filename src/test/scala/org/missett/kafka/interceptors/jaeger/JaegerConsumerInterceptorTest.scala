package org.missett.kafka.interceptors.jaeger

import io.opentracing.References
import io.opentracing.propagation.Format
import org.scalatest.{FlatSpec, Matchers}

class JaegerConsumerInterceptorTest extends FlatSpec with Matchers {
  behavior of "JaegerConsumerInterceptor"

  it should "report a span correctly when a message is consumed and the offset is committed" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    val rec = record(key = 1, value = 2)

    consume[Int, Int](rec, int)
    commit(int)

    val span = reporter.getSpans.get(0)

    span.getServiceName should equal (service)
    span.getOperationName should equal ("consume-and-commit")
    span.getTags.get("partition") should equal (rec.partition())
    span.getTags.get("offset") should equal (rec.offset())
    span.getTags.get("topic") should equal (rec.topic())
    span.isFinished should equal (true)
  }

  it should "track multiple spans being active at the same time on the same partition" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val one = record(key = 1, value = "one", partition = 1, offset = 1)
    val two = record(key = 2, value = "two", partition = 1, offset = 2)

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume(one, int)
    consume(two, int)

    int.spans.size should equal (2)

    commit(int, one.topic(), one.partition(), one.offset())
    commit(int, two.topic(), two.partition(), two.offset())

    val spans = reporter.getSpans

    spans.size() should equal (2)

    int.spans.size should equal (0)
  }

  it should "track multiple spans being active at the same time for the same offset on many partitions" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val one = record(key = 1, value = "one", partition = 1, offset = 1)
    val two = record(key = 2, value = "two", partition = 2, offset = 1)
    val three = record(key = 3, value = "three", partition = 3, offset = 1)
    val four = record(key = 4, value = "four", partition = 4, offset = 1)

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume(one, int)
    consume(two, int)
    consume(three, int)
    consume(four, int)

    int.spans.size should equal (4)

    commit(int, one.topic(), one.partition(), one.offset())
    commit(int, two.topic(), two.partition(), two.offset())
    commit(int, three.topic(), three.partition(), three.offset())
    commit(int, four.topic(), four.partition(), four.offset())

    val spans = reporter.getSpans

    spans.size() should equal (4)

    int.spans.size should equal (0)
  }

  it should "track multiple spans being active at the same time for the same offset on the same partition on many topics" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val one = record(key = 1, value = "one", partition = 1, offset = 1, topic = "one")
    val two = record(key = 2, value = "two", partition = 1, offset = 1, topic = "two")
    val three = record(key = 3, value = "three", partition = 1, offset = 1, topic = "three")
    val four = record(key = 4, value = "four", partition = 1, offset = 1, topic = "four")

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    consume(one, int)
    consume(two, int)
    consume(three, int)
    consume(four, int)

    int.spans.size should equal (4)

    commit(int, one.topic(), one.partition(), one.offset())
    commit(int, two.topic(), two.partition(), two.offset())
    commit(int, three.topic(), three.partition(), three.offset())
    commit(int, four.topic(), four.partition(), four.offset())

    val spans = reporter.getSpans

    spans.size() should equal (4)

    int.spans.size should equal (0)
  }

  it should "continue a trace that is received in the message headers" in new JaegerInterceptorTesting {
    val (reporter, _, tracer) = getTestComponents

    val int = new JaegerConsumerInterceptor
    int.tracer = tracer

    val rec = record(key = 1, value = "one")

    val parentSpan = tracer.buildSpan("parent-operation").start()
    val parentContext = parentSpan.context()

    tracer.inject(parentContext, Format.Builtin.TEXT_MAP, new ContextHeaderEncoder(rec.headers()))

    consume(rec, int)
    commit(int)

    val spans = reporter.getSpans
    spans.size() should equal (1)

    val span = spans.get(0)

    val refs = span.getReferences
    refs.size() should equal (1)

    val ref = refs.get(0)
    ref.getType should equal (References.FOLLOWS_FROM)
    ref.getSpanContext.getSpanId should equal (parentContext.getSpanId)
  }
}
