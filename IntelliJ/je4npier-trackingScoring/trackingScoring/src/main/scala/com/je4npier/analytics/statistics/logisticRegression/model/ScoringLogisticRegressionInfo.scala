package com.je4npier.analytics.statistics.logisticRegression.model

import com.je4npier.analytics.model.ScoringInfo

class ScoringLogisticRegressionInfo extends ScoringInfo {
  var group: ScoringLogisticRegressionInfoDbGroup = _
  var target: ScoringLogisticRegressionInfoDbTarget = _
}

private class ScoringLogisticRegressionInfoDbGroup {
  var fieldName: String = _
  var numberQuantiles: Integer = _
}

private class ScoringLogisticRegressionInfoDbTarget {
  var fieldName: String = _
  var valueTarget: Any = _
  var valueNoTarget: Any = _
}