package com.atguigu.online_edu.app

import com.alibaba.fastjson.JSON
import com.atguigu.online_edu.entity.VideoLog
import com.atguigu.online_edu.utils.{MyKafkaUtil, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object VideoTime {

  def main(args: Array[String]): Unit = {

    val topic = "course_learn_topic"
    val consumerId = "consumer01"

    val conf: SparkConf = new SparkConf().setAppName("question_right").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
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


    ////{"biz":"bdfb58e5-d14c-45d2-91bc-1d9409800ac3","chapterid":"1","cwareid":"3","edutypeid":"3","pe":"55","ps":"41","sourceType":"APP","speed":"2",
    ////	"subjectid":"2","te":"1563352166417","ts":"1563352159417","uid":"235","videoid":"2"}
    //VideoLog(1011767e-ae08-4411-a83b-3f6b68b3db3f,1,3,1,15,1,WeChat,2,1,1563352140114,1563352133114,432,5)
    val videoLodDS: DStream[VideoLog] = offsetRangesDS
      .map(_.value())
      .map(x => JSON.parseObject(x, classOf[VideoLog]))

    videoLodDS.cache()

    // TODO: 1.1. chapter总时长
    val chapter2TimeDS: DStream[(String, Long)] = videoLodDS.map(x => (x.chapterid,x.te.toLong - x.ts.toLong))

    val chapterStateDS: DStream[(String, Long)] = chapter2TimeDS.updateStateByKey {
      (values: Seq[Long], old: Option[Long]) =>

        var timeSum = 0L

        if (old.isDefined)
          timeSum = old.get

        for (elem <- values)
          timeSum += elem

        Option(timeSum)
    }

    // TODO: 1.2.cware总时长
    val cware2TimeDS: DStream[(String, Long)] = videoLodDS.map(x => (x.cwareid,x.te.toLong - x.ts.toLong))
    val cwareStateDS: DStream[(String, Long)] = cware2TimeDS.updateStateByKey {
      (values: Seq[Long], old: Option[Long]) =>
        var cnt = 0L

        if (old.isDefined)
          cnt = old.get

        for (elem <- values)
          cnt += elem

        Option(cnt)
    }


    // TODO: 1.3  edutypeid总时长
    val eduDS: DStream[(String, Long)] = videoLodDS.map(x => (x.edutypeid,x.te.toLong - x.ts.toLong))
    val eduStateDS: DStream[(String, Long)] = eduDS.updateStateByKey {
      (values: Seq[Long], old: Option[Long]) =>
        var cnt = 0L
        if (old.isDefined)
          cnt = old.get

        for (elem <- values) {
          cnt += elem
        }

        Option(cnt)
    }

    // TODO: 1.4 source总时长
    val sourceDS: DStream[(String, Long)] = videoLodDS.map(x => (x.sourceType,x.te.toLong - x.ts.toLong))
    val sourceStateDS: DStream[(String, Long)] = sourceDS.updateStateByKey {
      (values: Seq[Long], old: Option[Long]) =>
        var cnt = 0L
        if (old.isDefined)
          cnt = old.get

        for (elem <- values) {
          cnt += elem
        }

        Option(cnt)
    }


    // TODO: 1.5 subject总时长
    val subjectDS: DStream[(String, Long)] = videoLodDS.map(x => (x.subjectid,x.te.toLong - x.ts.toLong))
    val subjectStateDS: DStream[(String, Long)] = subjectDS.updateStateByKey {
      (values: Seq[Long], old: Option[Long]) =>
        var cnt = 0L
        if (old.isDefined)
          cnt = old.get

        for (elem <- values) {
          cnt += elem
        }

        Option(cnt)
    }


    // TODO: 2.



    sourceStateDS.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
