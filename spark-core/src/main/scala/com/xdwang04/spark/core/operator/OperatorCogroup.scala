package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorCogroup {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator cogroup")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("spark", 10), ("hadoop", 20), ("flink", 30), ("storm", 40)
    ))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("spark", 100), ("hadoop", 200), ("flink", 300), ("zookeeper", 400)
    ))

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cogroupRDD.collect().foreach(println)

    sc.stop()
  }

}
