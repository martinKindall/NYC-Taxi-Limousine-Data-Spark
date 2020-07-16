package org.myspark.operations

import java.sql.Timestamp

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TaxiStructuredOperations {
  def toSumIncrementsEventTime(sparkCtx: SparkSession, query: Dataset[Row]): Dataset[Row] = {
    import sparkCtx.implicits._

    query.withWatermark("timestamp", "60 seconds")
      .groupBy(
        window($"timestamp", "60 seconds", "10 seconds")
      )
      .sum("meter_increment").alias("total_money")
  }

  def toSessionWindowPerRide(sparkCtx: SparkSession, query: Dataset[Row]): Dataset[SessionUpdate] = {
    import sparkCtx.implicits._

    query
      .map(row => Event(
        row.getAs[String]("ride_id"),
        row.getAs[Timestamp]("timestamp"),
        row.getAs[String]("ride_status"),
        row.getAs[Float]("latitude"),
        row.getAs[Float]("longitude")
      ))
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
          if (state.hasTimedOut) {
            val finalUpdate = SessionUpdate(
              sessionId,
              state.get.latitude,
              state.get.longitude,
              state.get.startTimestampMs,
              state.get.durationMs,
              expired = true
            )
            state.remove()
            finalUpdate
          } else {
            val eventTimestamps = events.map(_.timestamp.getTime).toSeq
            val eventStartingLatLong: (Float, Float) = getStartingLatLong(events)
            val updatedSession = if (state.exists) {

            } else {
              SessionInfo(eventStartingLatLong._1, eventStartingLatLong._2, eventTimestamps.min, eventTimestamps.max)
            }

            SessionUpdate(
              sessionId,
              0.0f,
              0.0f,
              30L,
              30L,
              expired = false
            )
          }
      }
  }

  private def getStartingLatLong(events: Iterator[Event]): (Float, Float) = {
    val ridePickUpEvent = events.find(event => event.rideStatus == "pickup")
    val eventToLatLongPair = (firstEvent: Event) => {
      (firstEvent.latitude, firstEvent.longitude)
    }

    if (ridePickUpEvent.isEmpty) {
      eventToLatLongPair(events.toList.head)
    } else {
      eventToLatLongPair(ridePickUpEvent.get)
    }
  }
}


case class Event(sessionId: String,
                  timestamp: Timestamp,
                  rideStatus: String,
                  latitude: Float,
                  longitude: Float)

case class SessionInfo(latitude: Float,
                        longitude: Float,
                        startTimestampMs: Long,
                        endTimestampMs: Long) {
  def durationMs: Long = endTimestampMs - startTimestampMs
}

case class SessionUpdate(sessionId: String,
                          latitude: Float,
                          longitude: Float,
                          startTimestamp: Long,
                          durationMs: Long,
                          expired: Boolean)
