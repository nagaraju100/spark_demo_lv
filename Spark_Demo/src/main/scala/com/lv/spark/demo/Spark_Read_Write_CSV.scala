
package com.lv.spark.demo

import com.lv.spark.demo.SparkDemoOne.spark
import org.apache.spark.sql.SparkSession

/**
 * @author nagaraju.gajula
 *         created on Aug,06,2021
 */
object Spark_Read_Write_CSV  {

  def main(args: Array[String]): Unit = {
    print("Starting calling_csv_file")
    calling_csv_fil()
    print("End calling_csv_file")
  }

  def calling_csv_fil(): Unit ={
    val spark:SparkSession = SparkSession.builder()
       .master("local[1]")
      .appName("Spark_Read_Write_CSV")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.option("header",true).csv("src/main/resources/zipcodes.csv")
    df.printSchema()
    //val df_1 =df.toDF()
    //df_county = df_1.select("Country")
    df.createOrReplaceTempView("demo_zips")
    val df2 = spark.sql("select max(RecordNumber) as max_value, min(RecordNumber) as min_value from demo_zips")
    df2.write.option("header","true").mode("Overwrite").csv("tmp/spark_output/zipcodes2")
  }
}

