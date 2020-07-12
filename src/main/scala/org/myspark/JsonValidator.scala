package org.myspark

import play.api.libs.json.{JsResult, JsValue, Reads}

@SerialVersionUID(6529685098267757692L)
trait JsonValidator extends java.io.Serializable {

  def isValidRawJson(rawJson: String): Boolean
  def parse(rawJson: String): JsValue
  def toStructure[T](json: JsValue)(implicit fjs: Reads[T]): JsResult[T]
  def isValidStructure[T](json: JsValue)(implicit fjs: Reads[T]): Boolean
}
