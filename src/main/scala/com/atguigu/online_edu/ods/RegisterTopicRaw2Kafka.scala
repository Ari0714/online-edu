package com.atguigu.online_edu.ods

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.online_edu.entity.RegisterLog
import com.atguigu.online_edu.utils.MyKafkaSinkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{SparkConf, SparkContext}

object RegisterTopicRaw2Kafka {

  def main(args: Array[String]): Unit = {

    val topic = "register_topic"

    val conf: SparkConf = new SparkConf().setAppName("register_topic").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputDS: RDD[RegisterLog] = sc.textFile("G:\\100买\\20_尚硅谷大数据之在线教育\\1.笔记\\register.log")
      .map {
        x =>
          val strings: Array[String] = x.split("\t")
          RegisterLog(strings(0), strings(1), strings(2))
      }

    inputDS.foreach {
      x =>
        MyKafkaSinkUtil.send(topic, JSON.toJSONString(x, new SerializeConfig(true)))
    }


  }
}
