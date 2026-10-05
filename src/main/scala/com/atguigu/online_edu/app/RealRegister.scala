package com.atguigu.online_edu.app

import com.alibaba.fastjson.JSON
import com.atguigu.online_edu.entity.RegisterLog
import com.atguigu.online_edu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealRegister {

  def main(args: Array[String]): Unit = {

    val topic = "register_topic"
    val consumerId = "consumer01"


    val conf: SparkConf = new SparkConf().setAppName("real_register").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ch")

    // TODO: 1.获取kafka数据
    val kafkaDS: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, topic, consumerId)
      .map(_.value())

    val registerLogDS: DStream[RegisterLog] = kafkaDS.map(x => JSON.parseObject(x,classOf[RegisterLog]))

    val keyDS: DStream[(Int, Long)] = registerLogDS.map(x => (1,1L))

    // TODO: 2. updateStateByKey计算累计活跃
//    val stateDS: DStream[(Int, Long)] = keyDS.updateStateByKey {
//      (values: Seq[Long], old: Option[Long]) =>
//
//        var registerCnt = 0L
//
//        if (old.isDefined)
//          registerCnt = old.get
//
//        for (elem <- values) {
//          registerCnt += elem
//        }
//
//        Option(registerCnt)
//
//    }


    // TODO: 3.reduceByKeyAndWindow 每6秒统统计一次1分钟内的注册数据
    val windowDS: DStream[(Int, Long)] = keyDS.reduceByKeyAndWindow((a:Long,b:Long) => a+b,Seconds(60),Seconds(6))
    windowDS.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
