package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorMapValues {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator mapValues")
      .setMaster("local[*]")

    val path = getClass.getResource("/word.txt").getPath
    val sc = new SparkContext(sparkConf)
    val wordCount = sc.textFile(path)
      .flatMap(_.toLowerCase.split("\\W+"))
      .groupBy(word => word)
      .mapValues(_.size)

    wordCount.saveAsTextFile("spark-core/src/main/resources/save/")
    sc.stop()
  }

}
