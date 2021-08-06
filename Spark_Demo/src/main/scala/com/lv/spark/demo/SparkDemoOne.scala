
package com.lv.spark.demo

import org.apache.spark.sql.SparkSession

/**
 * @author nagaraju.gajula
 *         created on Aug,06,2021
 */
object SparkDemoOne extends App {
  println("Starting...")
  val spark:SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples.com")
    .getOrCreate()
  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.printSchema()
  dfFromRDD1.show()
}
