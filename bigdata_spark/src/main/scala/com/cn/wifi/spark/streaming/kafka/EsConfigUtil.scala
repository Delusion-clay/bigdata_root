package com.cn.wifi.spark.streaming.kafka

object EsConfigUtil {
    def getEsParams(idField:String): Map[String,String] ={
      Map[String,String](
        "es.nodes"->"hadoop-101",
        "es.port"->"9200",
        "es.clustername"->"apache-hadoop-es",
        "es.mapping.id"->idField
      )
    }
}
