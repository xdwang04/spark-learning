package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorCountByValue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator countByValue")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val countByValueRDD: collection.Map[Int, Long] = sc.makeRDD(Seq(1, 1, 1, 1, 3, 3, 3, 6, 6, 9, 10), 2)
      .countByValue()

    println(countByValueRDD)
    sc.stop()
  }

}
