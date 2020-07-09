package org.myspark

import org.myspark.dataTypes.TaxiRide
import play.api.libs.json.{JsResult, JsValue}

trait JsonValidator extends java.io.Serializable {
  def isValidRawJson(rawJson: String): Boolean
  def parse(rawJson: String): JsValue
  def toStructure(json: JsValue): JsResult[TaxiRide]
}
