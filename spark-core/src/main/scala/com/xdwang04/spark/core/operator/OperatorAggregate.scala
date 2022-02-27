package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorAggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator aggregate")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val aggregate: Int = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .aggregate(0)(_ + _, _ + _)

    println(aggregate)
    sc.stop()
  }

}
