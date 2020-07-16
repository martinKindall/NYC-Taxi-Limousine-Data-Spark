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
            val eventStartingLatLong: Option[(Float, Float)] = getPickupLatLong(events)

            val updatedSession = if (state.exists) {
              val oldSession = state.get
              val startLatLong = if (eventStartingLatLong.isEmpty) {
                (state.get.latitude, state.get.longitude)
              } else {
                eventStartingLatLong.get
              }
              SessionInfo(
                startLatLong._1,
                startLatLong._2,
                oldSession.startTimestampMs,
                math.max(oldSession.endTimestampMs, eventTimestamps.max))
            } else {
              val startLatLong = if (eventStartingLatLong.isEmpty) {
                events.toList.map(event => (event.latitude, event.longitude)).head
              } else {
                eventStartingLatLong.get
              }
              
              SessionInfo(
                startLatLong._1,
                startLatLong._2,
                eventTimestamps.min,
                eventTimestamps.max)
            }
            state.update(updatedSession)
            state.setTimeoutDuration("30 seconds")

            SessionUpdate(
              sessionId,
              state.get.latitude,
              state.get.longitude,
              30L,
              30L,
              expired = false
            )
          }
      }
  }

  private def getPickupLatLong(events: Iterator[Event]): Option[(Float, Float)] = {
    events.find(event => event.rideStatus == "pickup").map(event => {
      (event.latitude, event.longitude)
    })
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
