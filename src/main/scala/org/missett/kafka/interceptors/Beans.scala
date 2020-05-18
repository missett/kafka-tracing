package org.missett.kafka.interceptors

import java.lang.management.ManagementFactory

import javax.management.ObjectName

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
