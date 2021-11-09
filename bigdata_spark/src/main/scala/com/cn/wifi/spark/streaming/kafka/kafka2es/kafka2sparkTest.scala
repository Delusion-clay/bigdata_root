package com.cn.wifi.spark.streaming.kafka.kafka2es

import com.cn.wifi.kafka.config.KafkaConfig
import com.cn.wifi.spark.streaming.kafka.offset.ManageOffsetRedis
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._


object kafka2sparkTest extends Serializable with Logging {
  def main(args: Array[String]): Unit = {
    val topic = "wifi"
    val groupId = "kafka2spark"
    val sparkConf = new SparkConf().setAppName("kafka2sparkTest").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10L))
    val kafkaParms = KafkaConfig.getKafkaConfig(groupId)
    //从redis中获取偏移,只是在任务启动的时候
    val offsetMap = ManageOffsetRedis.getOffsetFromRedis(10, groupId, topic, kafkaParms.asInstanceOf[java.util.Map[String, String]])
    //offsets: collection.Map[TopicPartition, Long])
    val offsets = offsetMap.map(offset => {
      //对offsetMap中的每条数据进行转换
      val topicPartition = new TopicPartition(topic, offset._1.toInt)
      val offset_p = offset._2.toLong
      topicPartition -> offset_p
    }).toMap


    val DS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](offsets.keys.toList, kafkaParms, offsets))
    //ConsumerStrategies.Assign[String, String](offsets.keys.toList, kafkaParms, offsets))


    DS.foreachRDD(rdd => {
      //就是spark中保存的消费者的偏移信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //把偏移写到redis中去了
      ManageOffsetRedis.saveOffsetToRedis(10, groupId, offsetRanges)

      offsetRanges.foreach(offset => {
        println("===========================")
        println("offset.topic:" + offset.topic)
        println("offset.partition:" + offset.partition)
        println("offset.fromOffset:" + offset.fromOffset)
        println("offset.untilOffset:" + offset.untilOffset)
      })

      val rddNew = rdd.map(x => {
        val line = x.value()
        val map = new java.util.HashMap[String, String]()
        map.put("line", line)
        println(line)
        map
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

