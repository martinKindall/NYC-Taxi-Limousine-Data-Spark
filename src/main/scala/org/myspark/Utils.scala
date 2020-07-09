package org.myspark

import com.fasterxml.jackson.core.JsonParseException
import org.myspark.dataTypes.TaxiRide
import play.api.libs.json.{JsResult, JsValue, Json}


object Utils extends JsonValidator {
  override def isValidRawJson(rawJson: String): Boolean = {
    try {
      Json.parse(rawJson)
      true
    } catch {
      case e: JsonParseException => false
    }
  }

  override def parse(rawJson: String): JsValue = {
    Json.parse(rawJson)
  }

  override def toStructure(json: JsValue): JsResult[TaxiRide] = {
    Json.fromJson[TaxiRide](json)
  }
}
