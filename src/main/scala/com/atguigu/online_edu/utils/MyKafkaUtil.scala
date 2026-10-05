package com.atguigu.online_edu.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._

object MyKafkaUtil {

  private val pro: Properties = MyPropertiesUtil.load("config.properties")
  private val brokerList: AnyRef = pro.get("kafka.broker.list")

  // kafka消费者配置
  val kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> brokerList, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall_consumer_group01",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "earliest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  //使用param中定义的消费者组
  def getKafkaStream(ssc: StreamingContext, topic: String) = {

    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))

  }

  //使用自定义中定义的消费者组
  def getKafkaStream(ssc: StreamingContext, topic: String, consumer: String) = {
    kafkaParam("group.id") = consumer
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))

  }

  //使用自定义中定义的消费者组
  def getKafkaStream(ssc: StreamingContext, topic: String, consumer: String, offsets: Map[TopicPartition, Long]) = {
    kafkaParam("group.id") = consumer
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))

  }


  //根据是否有偏移量获取kafka数据
  def getFinalKafkaStream(ssc: StreamingContext, topic: String, consumer: String) = {

    val topicPartition: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, consumer)

    var inputDS: InputDStream[ConsumerRecord[String, String]] = null
    if (topicPartition != null && topic.size > 0)
      inputDS = MyKafkaUtil.getKafkaStream(ssc, topic, consumer)
    else
      inputDS = MyKafkaUtil.getKafkaStream(ssc, topic, consumer, topicPartition)

    var offsetRanges = Array.empty[OffsetRange]
    val offsetRangeDS: DStream[ConsumerRecord[String, String]] = inputDS.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val kakfaValueDS: DStream[String] = offsetRangeDS.map(_.value())

    (kakfaValueDS, offsetRanges)

  }

}
