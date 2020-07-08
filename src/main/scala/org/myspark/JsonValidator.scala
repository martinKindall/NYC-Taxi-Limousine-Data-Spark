package org.myspark

trait JsonValidator extends java.io.Serializable {
  def isValidRawJson(rawJson: String): Boolean
}
