package com.xdwang04.spark.sql.examples

import org.apache.spark.sql.SparkSession

case class Person(name: String, age: Long, address: String)

object GettingStarted2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    import spark.implicits._
    val caseClassDS = Seq(Person("Andy", 32, "北京市海淀区上地大街18号")).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    println(primitiveDS.map(_ + 10).collect().mkString(","))

    val peopleDS = spark
      .read
      .json("spark-sql/src/main/resources/person.json")
      .as[Person]

    peopleDS.show()

    val peopleDF = spark
      .sparkContext
      .textFile("spark-sql/src/main/resources/person.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0).trim, attributes(1).trim.toInt, attributes(2).trim))
      .toDF()

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 28 AND 38")
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

  }

}
