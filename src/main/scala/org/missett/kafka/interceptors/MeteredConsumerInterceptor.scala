package org.missett.kafka.interceptors

import java.lang.management.ManagementFactory
import java.util

import javax.management.ObjectName
import org.apache.kafka.clients.consumer.{ConsumerInterceptor, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

trait PromCounterMBean {
  def getName: String = getClass.getName
}

trait KafkaTopicPartitionBeanManager[A <: PromCounterMBean] {
  private var beans = Map[String, A]()

  val mbs = ManagementFactory.getPlatformMBeanServer

  def createBeanForTopicAndPartition(topic: String, partition: Int): A

  def getTopicAndPartitionBean(topic: String, partition: Int): A = {
    val key = s"$topic-$partition"

    beans.get(key) match {
      case Some(bean) =>
        bean
      case None =>
        val bean = createBeanForTopicAndPartition(topic, partition)
        beans += (key -> bean)
        bean
    }
  }
}

trait QueueTimeNanosCounterMBean extends PromCounterMBean {
  def getQueueTimeNanos: Long
  def getQueueTimeMillis: Long
}

class QueueTimeNanosCounter extends QueueTimeNanosCounterMBean {
  private var queueTimeNanos = 0L

  def addTimeNanos(t: Long): Unit = { queueTimeNanos += Math.max(0, t) }

  override def getQueueTimeNanos: Long = queueTimeNanos

  override def getQueueTimeMillis: Long = if(queueTimeNanos == 0L) 0L else queueTimeNanos / 1000000L
}

class QueueTimeNanosCounterBeanManager extends KafkaTopicPartitionBeanManager[QueueTimeNanosCounter] {
  override def createBeanForTopicAndPartition(topic: String, partition: Int): QueueTimeNanosCounter = {
    val queueTime = new QueueTimeNanosCounter
    mbs.registerMBean(queueTime, new ObjectName(s"org.missett.kafka.interceptors:type=QueueTimeNanosCounter,topic=$topic,partition=$partition"))
    queueTime
  }
}

trait MessagesIngestedCounterMBean extends PromCounterMBean {
  def getMessagesIngested: Long
}

class MessagesIngestedCounter extends MessagesIngestedCounterMBean {
  private var messagesIngested = 0L

  def incrementMessagesIngested(): Unit = { messagesIngested += 1 }

  override def getMessagesIngested: Long = messagesIngested
}

class MessagesIngestedCounterBeanManager extends KafkaTopicPartitionBeanManager[MessagesIngestedCounter] {
  override def createBeanForTopicAndPartition(topic: String, partition: Int): MessagesIngestedCounter = {
    val messagesIngested = new MessagesIngestedCounter
    mbs.registerMBean(messagesIngested, new ObjectName(s"org.missett.kafka.interceptors:type=MessagesIngestedCounter,topic=$topic,partition=$partition"))
    messagesIngested
  }
}

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