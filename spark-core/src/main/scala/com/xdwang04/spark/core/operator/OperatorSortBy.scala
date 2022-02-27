package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorSortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator sortBy")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val wordCount = sc.makeRDD(Seq(12, 33, 4, 67, 273, 55), 6)
      .sortBy(num => num, false)
      .coalesce(1)
      .saveAsTextFile("spark-core/src/main/resources/save/")

    sc.stop()
  }

}
