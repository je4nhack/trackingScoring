package com.je4npier.analytics.model

import java.util.Date

class ScoringInfo {
  var id: String = _
  var parent_id: String = _
  var name: String = _
  var implementationDate: Date = _
  var developmentEndDate: Date = _
  var referenceDate: Date = _
  var description: String = _
  var developerCompany: String = _
  var trainingAlgorithm: String = _
  var country: String = _
  var source: String = _
  var variables : Seq[VariableScoring] = _
  var audit: AuditFields = _
}
class AuditFields {
  var registrationDate: Date = _
  var registrationUser: String = _
  var lastUpdate_date: Date = _
  var lastUpdateDescription: String = _
  var lastUpdateUser: String = _
}