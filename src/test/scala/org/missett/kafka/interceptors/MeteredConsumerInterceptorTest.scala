package org.missett.kafka.interceptors

import java.lang.management.ManagementFactory

import javax.management.ObjectName
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}

trait TestTopology {
  val input = "input-topic"
  val output = "output-topic"

  implicit val StringSerde = Serdes.String
  implicit val StringSerializer = StringSerde.serializer()
  implicit val StringDeserializer = StringSerde.deserializer()
  implicit val IntSerde = Serdes.Integer
  implicit val IntSerializer = IntSerde.serializer()
  implicit val IntDeserializer = IntSerde.deserializer()
  implicit val Cons = Consumed.`with`[String, Int]
  implicit val Prod = Produced.`with`[String, Int]

  val builder = new StreamsBuilder()
  builder.stream[String, Int](input).mapValues(_ * 2).to(output)

  val config = Map("consumer.interceptor.classes" -> "org.missett.kafka.interceptors.MeteredConsumerInterceptor")
}

trait TestFixtures {
  def uuid: String = Gen.listOfN(36, Gen.alphaNumChar).map(_.mkString).sample.get

  trait TestImpMBean extends PromCounterMBean {
    def getTestMetric: Int
  }

  class TestImp extends TestImpMBean {
    private var metric: Int = 0

    def incMetric(): Unit = { metric += 1 }

    override def getTestMetric: Int = metric
  }

  class TestImpBeanManager extends KafkaTopicPartitionBeanManager[TestImp] {
    override def createBeanForTopicAndPartition(topic: String, partition: Int): TestImp = {
      val testImp = new TestImp
      mbs.registerMBean(testImp, new ObjectName(s"org.missett.kafka.interceptors:type=QueueTimeNanosCounter,topic=$topic,partition=$partition"))
      testImp
    }
  }
}

class MeteredConsumerInterceptorTest extends FlatSpec with Matchers with EmbeddedKafkaStreams with TestFixtures {
  behavior of "KafkaTopicPartitionBeanManager"

  it should "create a new bean for a topic and a partition that has never been seen before" in {
    val man = new TestImpBeanManager
    man.getTopicAndPartitionBean(uuid, 0).getTestMetric should equal (0)
  }

  it should "use the same bean multiple times when a message for the same topic and partition is seen" in {
    val man = new TestImpBeanManager
    val topic = uuid

    man.getTopicAndPartitionBean(topic, 0).getTestMetric should equal (0)
    man.getTopicAndPartitionBean(topic, 0).incMetric()
    man.getTopicAndPartitionBean(topic, 0).getTestMetric should equal (1)
  }

  it should "create separate beans for a topic with multiple partitions" in {
    val man = new TestImpBeanManager
    val topic = uuid

    man.getTopicAndPartitionBean(topic, 0).getTestMetric should equal (0)
    man.getTopicAndPartitionBean(topic, 0).incMetric()
    man.getTopicAndPartitionBean(topic, 0).getTestMetric should equal (1)

    man.getTopicAndPartitionBean(topic, 1).getTestMetric should equal (0)
    man.getTopicAndPartitionBean(topic, 1).incMetric()
    man.getTopicAndPartitionBean(topic, 1).getTestMetric should equal (1)
  }

  it should "create separate beans for two different topics with the same partition" in {
    val man = new TestImpBeanManager
    val topic1 = uuid
    val topic2 = uuid

    man.getTopicAndPartitionBean(topic1, 0).getTestMetric should equal (0)
    man.getTopicAndPartitionBean(topic1, 0).incMetric()
    man.getTopicAndPartitionBean(topic1, 0).getTestMetric should equal (1)

    man.getTopicAndPartitionBean(topic2, 0).getTestMetric should equal (0)
    man.getTopicAndPartitionBean(topic2, 0).incMetric()
    man.getTopicAndPartitionBean(topic2, 0).getTestMetric should equal (1)
  }

  behavior of "QueueTimeNanosCounter"

  it should "start with a counter set to 0" in {
    val bean = new QueueTimeNanosCounter
    bean.getQueueTimeNanos should equal (0)
    bean.getQueueTimeMillis should equal (0)
  }

  it should "allow user to add time" in {
    val bean = new QueueTimeNanosCounter
    bean.getQueueTimeNanos should equal (0)
    bean.addTimeNanos(1000000)
    bean.getQueueTimeNanos should equal (1000000)
  }

  it should "return nanos time converted to millis" in {
    val bean = new QueueTimeNanosCounter
    bean.getQueueTimeNanos should equal (0)
    bean.getQueueTimeMillis should equal (0)
    bean.addTimeNanos(1000000)
    bean.getQueueTimeMillis should equal (1)
  }

  it should "successfully return an odd number in millis" in {
    val bean = new QueueTimeNanosCounter
    bean.getQueueTimeNanos should equal (0)
    bean.addTimeNanos(11111111)
    bean.getQueueTimeMillis should equal (11)
  }

  it should "not change the counter if the given time is less than 0" in {
    val bean = new QueueTimeNanosCounter
    bean.getQueueTimeNanos should equal (0)
    bean.addTimeNanos(-1)
    bean.getQueueTimeMillis should equal (0)
  }

  behavior of "MessagesIngestedCounter"

  it should "start with a counter set to 0" in {
    val bean = new MessagesIngestedCounter
    bean.getMessagesIngested should equal (0)
  }

  it should "successfully increment the counter" in {
    val bean = new MessagesIngestedCounter
    bean.incrementMessagesIngested()
    bean.getMessagesIngested should equal (1)
  }

  behavior of "MeteredConsumerInterceptor"

  it should "process messages while creating the configured beans" in new TestTopology {
    runStreams(List(input, output), builder.build(), config) {
      publishToKafka(input, message = 1)
      val result = consumeFirstMessageFrom[Int](output)
      result should equal (2)

      val mbs = ManagementFactory.getPlatformMBeanServer

      val queueTimeNanosCounterName = new ObjectName("org.missett.kafka.interceptors:type=QueueTimeNanosCounter,topic=input-topic,partition=0")
      mbs.getAttribute(queueTimeNanosCounterName, "QueueTimeNanos")

      val messagesIngestedCounterName = new ObjectName("org.missett.kafka.interceptors:type=MessagesIngestedCounter,topic=input-topic,partition=0")
      mbs.getAttribute(messagesIngestedCounterName, "MessagesIngested")
    }
  }
}