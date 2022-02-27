package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorDisinct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator distinct")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1, 2, 4), 2)
    val distinct: RDD[Int] = rdd.distinct()
    val sum: Int = distinct.collect().sum

    println(sum)
    sc.stop()
  }

}
