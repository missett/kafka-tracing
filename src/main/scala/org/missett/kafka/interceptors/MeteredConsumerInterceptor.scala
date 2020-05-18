package org.missett.kafka.interceptors

import java.util

import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

class MeteredConsumerInterceptor extends ConsumerInterceptor[Array[Byte], Array[Byte]] {
  private def currentTimeNanos: Long = System.nanoTime()

  private def longToByteArray(x: Long): Array[Byte] = BigInt(x).toByteArray

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val queueTimeBeans = new QueueTimeNanosCounterBeanManager
  private val messagesIngestedBeans = new MessagesIngestedCounterBeanManager

  override def onConsume(records: ConsumerRecords[Array[Byte], Array[Byte]]): ConsumerRecords[Array[Byte], Array[Byte]] = {
    val iterator = records.iterator()

    while (iterator.hasNext) {
      val rec = iterator.next()

      val nowNanos = currentTimeNanos
      val recTimeNanos = rec.timestamp() * 1000
      val timeInQueueNanos = nowNanos - recTimeNanos

      queueTimeBeans.getTopicAndPartitionBean(rec.topic(), rec.partition()).addTimeNanos(timeInQueueNanos)
      messagesIngestedBeans.getTopicAndPartitionBean(rec.topic(), rec.partition()).incrementMessagesIngested()

      logger.info(s"ingested record at [$nowNanos] after being in queue for [$timeInQueueNanos]")

      rec.headers().add("ingestion-time-ns", longToByteArray(nowNanos))
    }

    records
  }

  override def onCommit(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = ()

  override def close(): Unit = ()

  override def configure(configs: util.Map[String, _]): Unit = ()
}