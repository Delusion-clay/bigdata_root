package com.cn.wifi.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: Delusion
 * @date: 2021-04-01 11:05
 */
object Test {
  def main(args: Array[String]): Unit = {
    println(f(7))
  }
  def f(num:Int): Int ={
    if(num==1 || num==2) 1 else f(num-1)+f(num-2)
  }
}
