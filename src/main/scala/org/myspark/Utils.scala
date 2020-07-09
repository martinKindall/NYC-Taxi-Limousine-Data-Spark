package org.myspark

import com.fasterxml.jackson.core.JsonParseException
import play.api.libs.json.{JsResult, JsValue, Json, Reads}


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

  override def toStructure[T](json: JsValue)(implicit fjs: Reads[T]): JsResult[T] = {
    Json.fromJson[T](json)
  }
}
