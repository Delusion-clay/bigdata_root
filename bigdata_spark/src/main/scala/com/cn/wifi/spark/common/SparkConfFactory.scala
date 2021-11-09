package com.cn.wifi.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-06-10 20:04
  */
object SparkConfFactory extends Serializable with Logging{

  /**
    *  sparkconf 工厂  本地模式
    * @param appName
    * @param threads
    */
     def newLocalSparkConf(appName:String="default",threads: Int=2): SparkConf ={
          new SparkConf().setAppName(appName).setMaster(s"local[${threads}]")
     }


  /**
    *  sparkconf 工厂 本地模式
    * @param appName
    */
  def newSparkConf(appName:String="default"): SparkConf ={
      new SparkConf().setAppName(appName)
  }

}
