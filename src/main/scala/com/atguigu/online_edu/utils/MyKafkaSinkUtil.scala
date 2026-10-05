package com.atguigu.online_edu.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaSinkUtil {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  private val brokers: String = properties.getProperty("kafka.broker.list")

  var producer:KafkaProducer[String,String] = null

  def getKafkaProducer: KafkaProducer[String,String] ={

    val properties = new Properties
    properties.put("bootstrap.servers", brokers)
    properties.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idempotence",(true: java.lang.Boolean))

    var kafkaProducer:KafkaProducer[String,String] = null

    try {
      kafkaProducer = new KafkaProducer[String, String](properties)
    } catch {
      case e:Exception =>
        e.printStackTrace()
    }

    kafkaProducer

  }

  def send(topic:String,msg:String): Unit ={

    if (producer == null)
      producer = getKafkaProducer
    producer.send(new ProducerRecord[String,String](topic,msg))

  }

  def send(topic:String,msg:String,key:String): Unit ={

    if (producer == null)
      producer = getKafkaProducer
    producer.send(new ProducerRecord[String,String](topic,key,msg))

  }

}
