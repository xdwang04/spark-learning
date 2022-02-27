package com.xdwang04.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator join")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("spark", 10), ("hadoop", 20), ("flink", 30), ("storm", 40)
    ))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("spark", 100), ("hadoop", 200), ("flink", 300), ("zookeeper", 400)
    ))

    val join: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    val left: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    val right: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    val full: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)

    println("------------------------------------------------")
    join.collect().foreach(println)
    println("------------------------------------------------")
    left.collect().foreach(println)
    println("------------------------------------------------")
    right.collect().foreach(println)
    println("------------------------------------------------")
    full.collect().foreach(println)

    sc.stop()
  }

}
