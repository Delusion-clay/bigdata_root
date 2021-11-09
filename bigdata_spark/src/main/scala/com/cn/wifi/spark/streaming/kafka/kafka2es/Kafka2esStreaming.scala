package com.cn.wifi.spark.streaming.kafka.kafka2es

import com.cn.wifi.es.admin.AdminUtil
import com.cn.wifi.es.client.ESclientUtil
import com.cn.wifi.kafka.config.KafkaConfig
import com.cn.wifi.spark.common.SscFactory
import com.cn.wifi.spark.common.convert.DataConvert
import com.cn.wifi.spark.streaming.kafka.EsConfigUtil
import com.cn.wifi.spark.streaming.kafka.utils.KafkaSparkUtil
import com.cn.wifi.time.TimeTranstationUtils
import org.elasticsearch.spark.rdd.EsSpark

/**
 * @description:
 *      索引设计：
 *        一个索引最多放多少数据？ 3-5亿
 *         每种类型数据每天1亿左右，存一年   365亿
 *         数据拆分
 * @author: Delusion
 * @date: 2021-03-25 19:44
 */
object Kafka2esStreaming {
  def main(args: Array[String]): Unit = {
    val topic = "wifi"
    val groupId = "Kafka2esStreaming"
    val ssc = SscFactory.newLocalSSC("Kafka2esStreaming", 3L)
    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)
    val kafkaSparkUtil = new KafkaSparkUtil(false)
    val resultDS = kafkaSparkUtil.getMapDSwithOffset(ssc,
      kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic).map(x => {
      //TODO 数据加工
      //时间戳转日期，加一个日期字段
      val collect_time = x.get("collect_time")
      val date = TimeTranstationUtils.Date2yyyyMMdd((collect_time + "000").toLong)
      x.put("dayPartion", date)
      //构造地理位置类型
      val longitude = x.get("longitude")
      val latitude = x.get("latitude")
      x.put("location", longitude + "," + latitude)
      x
    })
    //数据入ES之前 需要考慮所有的查詢需求
    //因为mapping定义好了是不能更改的
    //分表存放/
    //每种数据类型 每天数据量在1亿左右
    // 分表，分索引
    val array = Array("wechat", "mail", "qq") //過濾使用
    array.foreach(table => {
      //"wechat", "mail", "qq"
      //按数据类型过滤
      val tableDS = resultDS.filter(map => {
        table.equals(map.get("table"))
      })
      tableDS.print()
      tableDS.foreachRDD(rdd => {
        val client = ESclientUtil.getClient
        //拿到了所有的日期
        val arrayDays = rdd.map(x => (x.get("dayPartion"))).distinct().collect()
        arrayDays.foreach(day => {
          val table_day_RDD = rdd.filter(map => {
            day.equals(map.get("dayPartion"))
          }).map(map => {
            DataConvert.strMap2esObjectMap(map)
          })
          //创建动态索引
          val index = s"z_${table}_${day}"
          //创建索引，先判断是不是已经存在了 mapping
          if (!AdminUtil.indexExists(client, index)) {
            val path = s"es/mapping/${table}.json"
            AdminUtil.buildIndexAndMapping(index, index, path, 3, 1)
          }
          EsSpark.saveToEs(table_day_RDD, s"${index}/${index}", EsConfigUtil.getEsParams("id"))
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
