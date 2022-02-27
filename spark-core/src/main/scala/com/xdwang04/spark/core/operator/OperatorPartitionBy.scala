package com.xdwang04.spark.core.operator

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object OperatorPartitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator partitionBy")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      ("beijing", 100),
      ("shanghai", 200),
      ("nanjing", 300),
      ("hebei", 400)), 6)
      //.partitionBy(new HashPartitioner(2))
      .partitionBy(new MyPartitioner())
      .reduceByKey(_ + _)
      .saveAsTextFile("spark-core/src/main/resources/save/")

    sc.stop()
  }

}

class MyPartitioner extends Partitioner {
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    key match {
      case "beijing" => 0
      case "shanghai" => 1
      case _ => 2
    }
  }

}
