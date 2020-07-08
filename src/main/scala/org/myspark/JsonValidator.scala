package org.myspark

trait JsonValidator {
  def isValidRawJson(rawJson: String): Boolean
}
