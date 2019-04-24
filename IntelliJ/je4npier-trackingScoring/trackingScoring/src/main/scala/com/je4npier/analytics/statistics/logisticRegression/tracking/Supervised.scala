package com.je4npier.analytics.statistics.logisticRegression.tracking

import com.je4npier.analytics.statistics.logisticRegression.{ItemEvaluate, Target}
import com.je4npier.analytics.statistics.logisticRegression.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Supervised {

  def KolmogorovSmirnov(itemEvaluate: ItemEvaluate): Option[Double] = {
    var freqAbsTarget: Double = 0
    var freqAbsNoTarget: Double = 0
    if(itemEvaluate.getNTarget > 0 && itemEvaluate.getNNoTarget > 0){
      val ks = itemEvaluate.getDfAggGroup.collect.map(row => {
        freqAbsTarget += row.getAs[Long](labelFieldNameTarget).toDouble / itemEvaluate.getNTarget
        freqAbsNoTarget += row.getAs[Long](labelFieldNameNoTarget).toDouble / itemEvaluate.getNNoTarget
        Math.abs(freqAbsTarget - freqAbsNoTarget)
      }).max

      Some(ks)
    }else{
      None
    }
  }

  def KolmogorovSmirnov(df: DataFrame, target: Target, fieldNameGroup: String): Option[Double] =
    KolmogorovSmirnov(new ItemEvaluate(df, target, fieldNameGroup))

  def KolmogorovSmirnov(df: DataFrame, fieldNameTarget: String, fieldNameGroup: String): Option[Double] =
    KolmogorovSmirnov(new ItemEvaluate(df, fieldNameTarget, fieldNameGroup))

  def Gini(itemEvaluate: ItemEvaluate): Option[Double] = {
    var freqRelatNoTargetPrevious: Double = 0
    var freqAbsPlusRelatNoTarget: Double = 0

    if(itemEvaluate.getNTarget > 0 && itemEvaluate.getNNoTarget > 0){
      val gini = Math.abs(1 - itemEvaluate.getDfAggGroup.collect.map(row => {
        val freqRelatTarget: Double = row.getAs[Long](labelFieldNameTarget).toDouble / itemEvaluate.getNTarget
        val freqRelatNoTarget: Double = row.getAs[Long](labelFieldNameNoTarget).toDouble / itemEvaluate.getNNoTarget

        freqAbsPlusRelatNoTarget += freqRelatNoTargetPrevious + freqRelatNoTarget
        freqRelatNoTargetPrevious = freqRelatNoTarget

        freqRelatTarget * freqAbsPlusRelatNoTarget
      }).sum)

      Some (gini)
    }else{
      None
    }
  }

  def Gini(df: DataFrame, target: Target, fieldNameGroup: String): Option[Double] =
    Gini(new ItemEvaluate(df, target, fieldNameGroup))

  def Gini(df: DataFrame, fieldNameTarget: String, fieldNameGroup: String): Option[Double] =
    Gini(new ItemEvaluate(df, fieldNameTarget, fieldNameGroup))

  def InformationValue(itemEvaluate: ItemEvaluate): Option[Double] = {
    val columnFreqRelatTarget = col(labelFieldNameTotalTarget) / itemEvaluate.getNTarget
    val columnFreqRelatNoTarget = col(labelFieldNameTotalNoTarget) / itemEvaluate.getNNoTarget
    val columnWoe = log(columnFreqRelatTarget / columnFreqRelatNoTarget)
    val columnIv = columnWoe * (columnFreqRelatTarget - columnFreqRelatNoTarget)

    val iv = itemEvaluate.getDfAggGroup.
      agg(sum(columnIv)).first.get(0)

    if(Option(iv).isDefined) Some(iv.toString.toDouble) else None
  }

  def InformationValue(df: DataFrame, target: Target, fieldNameGroup: String): Option[Double] =
    InformationValue(new ItemEvaluate(df, target, fieldNameGroup))

  def InformationValue(df: DataFrame, fieldNameTarget: String, fieldNameGroup: String): Option[Double] =
    InformationValue(new ItemEvaluate(df, fieldNameTarget, fieldNameGroup))
}
