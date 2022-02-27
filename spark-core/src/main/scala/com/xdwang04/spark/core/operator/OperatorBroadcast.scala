package com.xdwang04.spark.core.operator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object OperatorBroadcast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator broadcast")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val map = mutable.Map[String, Int](("hadoop", 1000), ("spark", 2000), ("flink", 3000))
    val broadcastMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    val mapRDD: RDD[(String, (Int, Int))] = sc.makeRDD(List(("hadoop", 100), ("spark", 200), ("flink", 300)))
      .map {
        case (key, value) => {
          val cnt = broadcastMap.value.getOrElse(key, 0)
          (key, (value, cnt))
        }
      }

    mapRDD.collect().foreach(println)
    sc.stop()
  }

}
