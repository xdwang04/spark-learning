package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object OperatorGlom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator glom")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    glomRDD.collect().foreach(data => println(data.mkString(", ")))

    sc.stop()
  }
}
