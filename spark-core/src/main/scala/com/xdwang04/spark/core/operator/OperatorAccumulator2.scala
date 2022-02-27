package com.xdwang04.spark.core.operator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object OperatorAccumulator2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("operator accumulator2")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val myAcc = new MyAccumulator()
    sc.register(myAcc, "WC-ACCUMULATOR")
    val wordCount = sc.textFile(getClass.getResource("/word.txt").getPath)
      .flatMap(_.toLowerCase.split("\\W++"))
      .foreach(word => myAcc.add(word))

    val result: List[(String, Long)] = myAcc.value
      .toList
      .sortBy(_._2)(Ordering.Long.reverse)
      .take(10)

    println(result)
    sc.stop()
  }

}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
  private val wcMap = mutable.Map[String, Long]()

  override def isZero: Boolean = wcMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = this

  override def reset(): Unit = wcMap.clear()

  override def add(word: String): Unit = {
    val count: Long = wcMap.getOrElse(word, 0L) + 1
    wcMap.update(word, count)
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    other.value.foreach {
      case (word, count) => {
        val cnt: Long = wcMap.getOrElse(word, 0L) + count
        wcMap.update(word, cnt)
      }
    }
  }

  override def value: mutable.Map[String, Long] = wcMap
}
