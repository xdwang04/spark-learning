package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorCoalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator coalesce")
      .setMaster("local[*]")

    val path = getClass.getResource("/word.txt").getPath
    val sc = new SparkContext(sparkConf)
    val wordCount = sc.textFile(path, 6)
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .coalesce(1)
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .saveAsTextFile("spark-core/src/main/resources/save/")

    sc.stop()
  }

}
