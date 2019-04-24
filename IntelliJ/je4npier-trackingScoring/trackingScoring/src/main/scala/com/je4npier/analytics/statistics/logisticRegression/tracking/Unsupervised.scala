package com.je4npier.analytics.statistics.logisticRegression.tracking

import com.je4npier.analytics.statistics.logisticRegression.{ItemEvaluate, Target}
import com.je4npier.analytics.statistics.logisticRegression.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, log, sum}

object Unsupervised {

  def PopulationStabilityIndex(itemEvaluateReference: ItemEvaluate, itemEvaluateToCompare: ItemEvaluate): Option[Double] = {
    val labelDfReference = "reference"
    val labelDfToCompare = "toCompare"

    val dfAggGroupReference = itemEvaluateReference.getDfAggGroup.persist()
    val dfAggGroupToCompare = itemEvaluateToCompare.getDfAggGroup.persist()

    dfAggGroupReference.select(labelFieldNameGroup).show()
    dfAggGroupToCompare.select(labelFieldNameGroup).show()

    val setGroupsReference = dfAggGroupReference.select(labelFieldNameGroup).collect.map(_.getString(0)).toSet
    val setGroupsToCompare = dfAggGroupToCompare.select(labelFieldNameGroup).collect.map(_.getString(0)).toSet

    if(setGroupsReference == setGroupsToCompare){
      val dfAggJoin = dfAggGroupReference.alias(labelDfReference).
        join(dfAggGroupToCompare.alias(labelDfToCompare), Seq(labelFieldNameGroup), "left_outer")

      val nTotalReference = itemEvaluateReference.getNTarget + itemEvaluateReference.getNNoTarget
      val nTotalToCompare = itemEvaluateToCompare.getNTarget + itemEvaluateToCompare.getNNoTarget

      val columnTotalReference = col(s"$labelDfReference.$labelFieldNameTotalTarget") + col(s"$labelDfReference.$labelFieldNameTotalNoTarget")
      val columnTotalToCompare = col(s"$labelDfToCompare.$labelFieldNameTotalTarget") + col(s"$labelDfToCompare.$labelFieldNameTotalNoTarget")

      val columnFreqRelatReference = columnTotalReference / nTotalReference
      val columnFreqRelatToEvaluate = columnTotalToCompare / nTotalToCompare

      val columnLogDivisionFreqRelat = log(columnFreqRelatReference / columnFreqRelatToEvaluate)
      val columnDifferenceFreqRelat = columnFreqRelatReference - columnFreqRelatToEvaluate
      val columnIe = columnLogDivisionFreqRelat * columnDifferenceFreqRelat

      val ie = dfAggJoin.agg(sum(columnIe)).first.get(0)

      dfAggGroupReference.unpersist()
      dfAggGroupToCompare.unpersist()

      if(Option(ie).isDefined) Some(ie.toString.toDouble) else None

    }else{
      None
    }
  }

  def PopulationStabilityIndex(dfReference: DataFrame, targetReference: Target, fieldNameGroupReference: String,
                               dfToCompare: DataFrame, targetToCompare: Target, fieldNameGroupToCompare: String): Option[Double] =
    PopulationStabilityIndex(new ItemEvaluate(dfReference, targetReference, fieldNameGroupReference),
                              new ItemEvaluate(dfToCompare, targetToCompare, fieldNameGroupToCompare))

  def PopulationStabilityIndex(dfReference: DataFrame, fieldNameTargetReference: String, fieldNameGroupReference: String,
                               dfToCompare: DataFrame, fieldNameTargetToCompare: String, fieldNameGroupToCompare: String): Option[Double] =
    PopulationStabilityIndex(new ItemEvaluate(dfReference, fieldNameTargetReference, fieldNameGroupReference),
                              new ItemEvaluate(dfToCompare, fieldNameTargetToCompare, fieldNameGroupToCompare))
}
