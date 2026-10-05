package com.atguigu.online_edu.ods

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.online_edu.utils.MyKafkaSinkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LearnRaw2Kafka {

  def main(args: Array[String]): Unit = {

    val topic = "course_learn_topic"

    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputDS: RDD[String] = sc.textFile("G:\\100买\\20_尚硅谷大数据之在线教育\\2.资料\\01日志数据\\04_实时-kafka主题数据\\course_learn.log", 4)

    inputDS.foreach {
      x =>
        MyKafkaSinkUtil.send(topic, x)
    }


  }
}
