package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorCache {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator cache")
      .setMaster("local[*]")

    val path = getClass.getResource("/word.txt").getPath
    val sc = new SparkContext(sparkConf)
    val mapRDD: RDD[(String, Int)] = sc.textFile(path)
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .cache()

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    val aggregateByKeyRDD: RDD[(String, Int)] = mapRDD.aggregateByKey(0)(_ + _, _ + _)
    val foldByKeyRDD: RDD[(String, Int)] = mapRDD.foldByKey(0)(_ + _)
    val combineByKeyRDD: RDD[(String, Int)] = mapRDD.combineByKey(
      v => v,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y,
    )

    reduceByKeyRDD.collect().foreach(println)
    aggregateByKeyRDD.collect().foreach(println)
    foldByKeyRDD.collect().foreach(println)
    combineByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
