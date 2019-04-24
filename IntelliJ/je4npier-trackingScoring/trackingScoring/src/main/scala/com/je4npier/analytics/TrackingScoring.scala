package com.je4npier.analytics

import com.je4npier.analytics.statistics.logisticRegression._
import com.je4npier.analytics.statistics.logisticRegression.tracking.Supervised._
import com.je4npier.analytics.statistics.logisticRegression.tracking.Unsupervised._
import com.je4npier.analytics.util.Je4nApp
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object TrackingScoring extends Je4nApp {
  override def run(spark: SparkSession, config: Config): Unit = {
    val df = spark.read.
      parquet("/mnt/backup-ubuntu/WorkspaceDocker/docker-compose/je4n-tesis/jupyter/work/data/parquet/buro_pj/cutoff_date=2017-06-30")

    val df2 = spark.read.
      parquet("/mnt/backup-ubuntu/WorkspaceDocker/docker-compose/je4n-tesis/jupyter/work/data/parquet/buro_pj/cutoff_date=2017-05-31")

    val itemEvaluate = new ItemEvaluate(df, "FLG_BUENO_MALO", "GRRIESGO")
    val itemEvaluate2 = new ItemEvaluate(df2, "FLG_BUENO_MALO", "GRRIESGO")

    val gini = Gini(itemEvaluate)
    val ks = KolmogorovSmirnov(itemEvaluate)
    val iv = InformationValue(itemEvaluate)
    val ie = PopulationStabilityIndex(itemEvaluate, itemEvaluate2)


    println("GINI: " + gini.getOrElse(-1))
    println("KS: " + ks.getOrElse(-1))
    println("IV: " + iv.getOrElse(-1))
    println("IE: " + ie.getOrElse(-1))
  }
}