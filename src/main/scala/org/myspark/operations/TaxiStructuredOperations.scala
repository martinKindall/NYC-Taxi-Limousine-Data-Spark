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
        row.getAs[String]("ride_status")
      ))
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
          if (state.hasTimedOut) {
            SessionUpdate(
              sessionId,
              0.0f,
              0.0f,
              Timestamp.valueOf("2020-10-10 20:00:00"),
              30L,
              expired = true
            )
          } else {
            SessionUpdate(
              sessionId,
              0.0f,
              0.0f,
              Timestamp.valueOf("2020-10-10 20:00:00"),
              30L,
              expired = false
            )
          }
      }
  }
}


case class Event(sessionId: String, timestamp: Timestamp, rideStatus: String)

case class SessionInfo(
                        latitude: Float,
                        longitude: Float,
                        startTimestamp: Timestamp,
                        endTimestamp: Timestamp) {
  def durationMs: Long = endTimestamp.getTime - startTimestamp.getTime
}

case class SessionUpdate(
                          sessionId: String,
                          latitude: Float,
                          longitude: Float,
                          startTimestamp: Timestamp,
                          durationMs: Long,
                          expired: Boolean)
