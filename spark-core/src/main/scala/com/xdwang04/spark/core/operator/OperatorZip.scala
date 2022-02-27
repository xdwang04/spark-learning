package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorZip {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator subtract")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val rdd2 = sc.makeRDD(List(4, 5, 6, 7, 8, 9))
    val rdd: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd.collect().mkString(", "))

    sc.stop()
  }

}
