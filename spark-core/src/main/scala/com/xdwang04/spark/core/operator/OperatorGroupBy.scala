package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorGroupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator groupBy")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    val sum = rdd.groupBy(_ % 2)
      .map {
        case (x, y) => {
          y.sum
        }
      }
      .collect()
      .sum

    println(sum)
    sc.stop()
  }

}
