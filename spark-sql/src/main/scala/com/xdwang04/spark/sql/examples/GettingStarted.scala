package com.xdwang04.spark.sql.examples

import org.apache.spark.sql.SparkSession

object GettingStarted {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.json("spark-sql/src/main/resources/person.json")

    df.show()
    println("---------------------------------------------------")
    df.printSchema()
    println("---------------------------------------------------")
    df.select("name").show()
    println("---------------------------------------------------")
    df.select($"name", $"age" + 10 as "Age").show()
    println("---------------------------------------------------")
    df.filter($"age" > 36).show()
    println("---------------------------------------------------")
    df.groupBy("age").count().show()
    println("---------------------------------------------------")
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql(
      """
        | SELECT
        |     name,
        |     age
        | FROM
        |     people
        | WHERE
        |     age >= 36
        |""".stripMargin)
    sqlDF.show()
  }

}
