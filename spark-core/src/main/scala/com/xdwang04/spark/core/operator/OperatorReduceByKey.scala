package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorReduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator reduceByKey")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello kafka", "hello zookeeper"))
    val wordCount: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordCount.collect().foreach(println)
    sc.stop()
  }

}
