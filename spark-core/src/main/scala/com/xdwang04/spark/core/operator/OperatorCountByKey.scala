package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorCountByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator countByKey")
      .setMaster("local[*]")

    val path = getClass.getResource("/word.txt").getPath
    val sc = new SparkContext(sparkConf)
    val wordCount: List[(String, Long)] = sc.textFile(path)
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1L))
      .countByKey()
      .toList
      .sortBy(_._2)
      .reverse
      .take(10)

    println(wordCount)
    sc.stop()
  }

}
