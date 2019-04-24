package com.je4npier.analytics.util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

trait Je4nApp {

  def main(args: Array[String]): Unit ={
    val config: Config = ConfigFactory.parseFile(new File(args.head))

    val configMongo: Config = config.getConfig("configMongo")

    if(Option(configMongo).isEmpty){
      val spark = SparkSession.builder().getOrCreate()
      run(spark, config)
    }else{
      val spark = SparkSession.builder().
        appName("example-spark-scala-read-and-write-from-mongo").
        //Configuración URI Mongo
        config("spark.mongodb.input.uri", configMongo.getString("uriInput")).
        config("spark.mongodb.output.uri", configMongo.getString("uriOutput")).
        //Configuración de colección de lectura y escritura
        config("spark.mongodb.input.collection", configMongo.getString("collectionInput")).
        config("spark.mongodb.output.collection", configMongo.getString("collectionOutput")).
        //Tipo de particionado para la transformación de doumentos a dataframe
        config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner").
        //Número de particiones en el resultado del dataframe
        config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "1").
        getOrCreate()
      run(spark, config)
    }
  }
  protected def run(spark: SparkSession, config: Config) {}
}
