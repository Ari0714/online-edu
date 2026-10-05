package com.atguigu.online_edu.app

import com.alibaba.fastjson.JSON
import com.atguigu.online_edu.entity.{QzLog, RegisterLog}
import com.atguigu.online_edu.utils.{MyKafkaUtil, MyRedisUtil, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

object QuestionRight {

  def main(args: Array[String]): Unit = {

    val topic = "qz_topic"
    val consumerId = "consumer01"

    val conf: SparkConf = new SparkConf().setAppName("question_right").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ch02")


    // TODO: 1.获取kafka数据
    val topicPartition: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, consumerId)

    var inputDS: InputDStream[ConsumerRecord[String, String]] = null
    if (topicPartition != null && topic.size > 0)
      inputDS = MyKafkaUtil.getKafkaStream(ssc, topic, consumerId)
    else
      inputDS = MyKafkaUtil.getKafkaStream(ssc, topic, consumerId, topicPartition)

    var offsetRanges = Array.empty[OffsetRange]
    val offsetRangesDS: DStream[ConsumerRecord[String, String]] = inputDS.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }


    //{"acknowledgeId":"24","courseId":"501","createTime":"2019-07-12 11:17:47","isRight":"1","questionId":"8","userId":"1004"}
    val qzLogDS: DStream[QzLog] = offsetRangesDS.map(x => JSON.parseObject(x.value(), classOf[QzLog]))

    // TODO: 2.去重
    //rdd去重
    val keyDS: DStream[(String, QzLog)] = qzLogDS.map {
      x =>
        val key: String = x.userId + "_" + x.courseId + "_" + x.acknowledgeId
        (key, x)
    }
      .transform(rdd => rdd.distinct())


    //借助redis去重
    //    keyDS.transform{
    //      rdd => {
    //        val groupbyRDD: RDD[(String, Iterable[QzLog])] = rdd.groupByKey()
    //        for ((key,iter) <- groupbyRDD){
    //          for (elem <- iter) {
    //
    //          }
    //        }
    //
    //
    //        groupbyRDD
    //      }
    //    }

    //    keyDS.transform{
    //      rdd =>
    //        rdd.foreachPartition{
    //          iter => {
    //
    //            val jedis: Jedis = MyRedisUtil.getJedisClient()
    //
    //            for (elem <- iter) {
    //              jedis.sadd(elem._1,)
    //            }
    //
    //            iter
    //          }
    //
    //        }
    //    }


    qzLogDS.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
