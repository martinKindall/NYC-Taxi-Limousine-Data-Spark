package org.myspark.dataTypes

import play.api.libs.json.{Reads, JsPath}
import play.api.libs.functional.syntax._

case class TaxiRide(rideId: String, pointIdx: Int, latitude: Float, longitude: Float)

object TaxiRide {
  implicit val taxi: Reads[TaxiRide] = (
    (JsPath \ "ride_id").read[String] and
      (JsPath \ "point_idx").read[Int] and
      (JsPath \ "latitude").read[Float] and
      (JsPath \ "longitude").read[Float]
    )(TaxiRide.apply _)
}
