package com.xdwang04.spark.core.examples

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("word count")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val wordCount = sc.makeRDD(List("hello spark", "hello kafka", "hello zookeeper"))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordCount.collect().foreach(println)
    sc.stop()
  }

}
