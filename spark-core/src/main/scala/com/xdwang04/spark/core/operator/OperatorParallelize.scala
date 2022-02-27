package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorParallelize {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator parallelize")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val wordCount = sc.parallelize(Seq("hello spark", "hello hadoop", "hello flink"), 4)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordCount.collect().foreach(println)
    sc.stop()
  }

}
