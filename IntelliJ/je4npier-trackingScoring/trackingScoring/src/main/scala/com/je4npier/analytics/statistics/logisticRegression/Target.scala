package com.je4npier.analytics.statistics.logisticRegression

class Target (val fieldNameTarget: String, val valueTarget: Any, val valueNoTarget: Any) {

  def this(fieldNameTarget: String, valueTarget: Any){
    this(fieldNameTarget, valueTarget, null)
  }
}
