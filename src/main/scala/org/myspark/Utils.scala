package org.myspark

import com.fasterxml.jackson.core.JsonParseException
import play.api.libs.json.Json


object Utils extends JsonValidator {
  def isValidRawJson(rawJson: String): Boolean = {
    try {
      Json.parse(rawJson)
      true
    } catch {
      case e: JsonParseException => false
    }
  }
}
