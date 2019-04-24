package com.je4npier.analytics

import java.sql.Date
import java.text.SimpleDateFormat

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}

object PruebaHadoop extends App {
  val spark = SparkSession.builder().getOrCreate()
  pruebaEscritura

  def pruebaEscritura = {
    val dataset= List(
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("170831").getTime()), "L", "08352743", "M"),
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("170831").getTime()), "L", "32549734", "F"),
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("170930").getTime()), "L", "32549734", "F"),
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("170930").getTime()), "R", "45238767", null),
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("171031").getTime()), "R", "09873426", null),
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("171031").getTime()), "L", "32549734", "F"),
      Row(new Date(new SimpleDateFormat("yyMMdd").parse("171031").getTime()), "R", "45238767", null)
    )
    val schema = StructType(Array(
      StructField("cutoff_date", DateType, false),
      StructField("personal_type", StringType, false),
      StructField("personal_id", StringType, false),
      StructField("gender_type", StringType, true)
    )
    )
    val rdd = spark.sparkContext.parallelize(dataset)
    val df = spark.createDataFrame(rdd, schema)
    df.show
    df.write.mode("overwrite").option("header", "true").csv("hdfs://hadoop:9000/data/csv/prubitaConfe")
  }

  def pruebaLectura: Unit ={
    val df = spark.read.parquet("hdfs://localhost:9000/data/master/pdco/data/retailBusinessBanking/v_pdco_payroll_mph_mov")
    df.show(5)
    df.limit(5).write.mode("overwrite").option("header", "true").csv("hdfs://hadoop:9000/data/csv/prubitaConfe2")
  }

}