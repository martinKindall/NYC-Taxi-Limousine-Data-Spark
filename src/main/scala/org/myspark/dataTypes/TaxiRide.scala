package org.myspark.dataTypes

import play.api.libs.json.{Reads, JsPath}
import play.api.libs.functional.syntax._

case class TaxiRide(ride_id: String, pointIdx: Int)

object TaxiRide {
  implicit val taxi: Reads[TaxiRide] = (
    (JsPath \ "ride_id").read[String] and
      (JsPath \ "point_idx").read[Int]
    )(TaxiRide.apply _)
}
