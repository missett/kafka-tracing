package io.github.missett.kafkatracing.jaeger.analytics.streams

import io.github.missett.kafkatracing.jaeger.analytics.LazyLogging
import io.github.missett.kafkatracing.jaeger.analytics.model.Span
import io.github.missett.kafkatracing.jaeger.analytics.model.TraceGraph.{TraceGraphAlgebra, TraceGraphAlgebraImp}
import io.github.missett.kafkatracing.jaeger.analytics.serialization.KafkaStoreAccess
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import scala.collection.JavaConverters._

class SpanProcessor(tracegraph: TraceGraphAlgebra) extends Processor[String, Span] with LazyLogging {
  var context: ProcessorContext = _

  val store = new KafkaStoreAccess[String, Span](this.context.getStateStore(TopologyLabels.SpanStoreName).asInstanceOf[KeyValueStore[String, Span]])
  val engine = new GremlinGroovyScriptEngine()
  val query =
    s"""g.V()
       |.has("span", "serviceName", "bottom-row-app-1")
       |.as("end")
       |.out("FOLLOWS_FROM")
       |.has("span", "serviceName", "middle-row-app-1")
       |.as("middle")
       |.out("FOLLOWS_FROM")
       |.has("span", "serviceName", "top-row-app-1")
       |.as("start")
       |""".stripMargin

  override def init(context: ProcessorContext): Unit = { this.context = context; () }

  override def process(key: String, span: Span): Unit = {
    logger.info(s"recieved span [$key] -> [$span]")

    store.set(span.spanId, span)

    val graph = tracegraph.create(store, span)

    tracegraph.execute(engine, graph, query) match {
      case Right(result) =>
        result.toList.asScala.map(n => (n.get("start"), n.get("end"))).foreach { case (start, end) =>
          val startTime = start.property[String]("startTime").value()
          val endTime = end.property[String]("startTime").value()

          logger.info(s"trace began at ${startTime} and ended at ${endTime}")
        }
      case Left(error)   =>
        logger.warn(s"error executing graph query", error)
    }

    span.references.foreach(refs => {
      refs.foreach(ref => {
        if (store.get(ref.spanId).isEmpty) {
          logger.warn(s"span is orphaned, and missing ${ref.spanId}")
        }
      })
    })

    ()
  }

  override def close(): Unit = ()
}

class SpanProcessorSupplier extends ProcessorSupplier[String, Span] {
  override def get(): Processor[String, Span] = new SpanProcessor(new TraceGraphAlgebraImp)
}