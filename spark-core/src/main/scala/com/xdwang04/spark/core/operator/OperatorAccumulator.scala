package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator accumulator")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val acc = sc.longAccumulator("long-accumulator")

    val mapRDD: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .map(num => {
        acc.add(num)
        num
      })

    mapRDD.collect()
    println(acc.value)
    sc.stop()
  }

}
