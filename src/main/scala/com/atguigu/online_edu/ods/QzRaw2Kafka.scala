package com.atguigu.online_edu.ods

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.online_edu.entity.QzLog
import com.atguigu.online_edu.utils.MyKafkaSinkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object QzRaw2Kafka {

  def main(args: Array[String]): Unit = {

    val topic = "qz_topic"

    val conf: SparkConf = new SparkConf().setAppName("qz").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputDS: RDD[QzLog] = sc.textFile("G:\\100买\\20_尚硅谷大数据之在线教育\\1.笔记\\qz.log")
      .map {
        x =>
          val strings: Array[String] = x.split("\t")
          QzLog(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
      }

    inputDS.foreach {
      x =>
        MyKafkaSinkUtil.send(topic, JSON.toJSONString(x, new SerializeConfig(true)))
    }


  }
}
