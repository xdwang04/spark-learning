package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorFoldByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator foldByKey")
      .setMaster("local[*]")

    val path = getClass.getResource("/word.txt").getPath
    val sc = new SparkContext(sparkConf)
    val wordCount: Array[(String, Int)] = sc.textFile(path)
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .foldByKey(0)(_ + _)
      .sortBy(_._2, false)
      .take(10)

    println(wordCount.mkString(", "))
    sc.stop()
  }

}
