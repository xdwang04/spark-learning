package com.xdwang04.spark.core.operator

import org.apache.spark.{SparkConf, SparkContext}

object OperatorMapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator mapPartitions")
      .setMaster("local[*]")

    val path = getClass.getResource("/word.txt").getPath
    val sc = new SparkContext(sparkConf)
    val wordCount = sc.textFile(path)
      .mapPartitions(it => {
        it.flatMap(_.toLowerCase.split("\\W+"))
          .map((_, 1))
      })
      .reduceByKey(_ + _)
      .sortBy(_._2, true)

    wordCount.saveAsTextFile("spark-core/src/main/resources/save/")
    sc.stop()
  }

}
