package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorReduce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator reduce")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val reduce: Int = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .reduce(_ + _)

    println(reduce)
    sc.stop()
  }

}
