package com.je4npier.analytics.statistics.logisticRegression

import Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when}

class ItemEvaluate (val df: DataFrame, val target: Target, val fieldNameGroup: String) {

  private var nTarget: Int = _
  private var nNoTarget: Int = _
  private var nNull: Int = _
  private var nTotal: Int = _

  private val columnTarget = when(col(target.fieldNameTarget) === target.valueTarget, 1)
  private val columnNoTarget = if(Option(target.valueNoTarget).isEmpty)
      when(col(target.fieldNameTarget) =!= target.valueTarget, 1)
    else when(col(target.fieldNameTarget) === target.valueNoTarget, 1)

  private val columnNull = when(columnTarget.isNull and columnNoTarget.isNull, 1)

  //Actualizamos los totales
  setTotals()

  def this(df: DataFrame, fieldNameTarget: String, fieldNameGroup: String) =
    this(df, new Target(fieldNameTarget, 1), fieldNameGroup)

  def setTotals(): Unit = {
    val dfAgg: DataFrame = df.
      agg(count(columnTarget),
        count(columnNoTarget),
        count(columnNull))

    val rowTotals = dfAgg.first

    this.nTarget = rowTotals.getLong(0).toInt
    this.nNoTarget = rowTotals.getLong(1).toInt
    this.nNull = rowTotals.getLong(2).toInt
    this.nTotal = nTarget + nNoTarget + nNull
  }
  
  def getDfSummary: DataFrame = df.
    select(col(fieldNameGroup).alias(labelFieldNameGroup),
      columnTarget.alias(labelFieldNameTarget),
      columnNoTarget.alias(labelFieldNameNoTarget),
      columnNull.alias(labelFieldNameNull))
  def getDfAggGroup: DataFrame = getDfSummary.groupBy(labelFieldNameGroup).
    agg(count(labelFieldNameTarget).alias(labelFieldNameTotalTarget),
      count(labelFieldNameNoTarget).alias(labelFieldNameTotalNoTarget),
      count(labelFieldNameNull).alias(labelFieldNameTotalNull)).
    sort(labelFieldNameGroup)

  def getNTarget: Int = nTarget
  def getNNoTarget: Int = nNoTarget
  def getNNull: Int = nNull
  def getNNtotal: Int = nTotal
}
