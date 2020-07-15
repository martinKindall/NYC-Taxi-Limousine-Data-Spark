package org.myspark

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.myspark.dataTypes.TaxiRide
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}


@SerialVersionUID(6529685098267757690L)
class TaxiOperations(taxiStruct: StructType) extends java.io.Serializable {

  def parseDStreamTaxiSessionWindows(sparkCtx: SparkSession, dsTaxiStream: DStream[TaxiRide]): Unit = {
    import sparkCtx.implicits._

    dsTaxiStream
      .map(taxiData => {
        Event(taxiData.rideId, Timestamp.valueOf(taxiData.timestamp), taxiData.rideStatus)
      })
      .foreachRDD(rdd => {
        val streamUpdates =
          rdd
          .toDF()
          .groupByKey(row => row.getAs[String]("sessionId"))
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

        streamUpdates
          .writeStream
          .outputMode("update")
          .format("console")
          .start()
      })
  }

  def parseDStreamTaxiSumIncrements(dsTaxiStream: DStream[TaxiRide]): DStream[String] = {
    dsTaxiStream
      .map(taxiData => {
        taxiData.meterIncrement
      })
      .reduceByWindow((amount1: Float, amount2: Float) => amount1 + amount2,
        Seconds(60), Seconds(3))
      .map(totalSum => s"{'dollar_per_minute': $totalSum}")
  }

  def parseDStreamTaxiSumIncrementsEventTime(sparkCtx: SparkSession,
                                             dsTaxiStream: DStream[TaxiRide],
                                             applyOnDF: Dataset[String] => Unit): Unit = {
    import sparkCtx.implicits._

    dsTaxiStream
      .foreachRDD(rdd => {
        applyOnDF(
          rdd.toDF().withWatermark("timestamp", "60 seconds")
            .groupBy(
              window($"timestamp", "60 seconds", "10 seconds")
            )
            .sum("meterIncrement")
            .toJSON
        )
      })
  }

  def parseDStreamTaxiCountRides(dsTaxiStream: DStream[TaxiRide]): DStream[String] = {
    val aggregatedCount = dsTaxiStream
      .map(taxiData => {
        val roundedLat = truncateLatLong(taxiData.latitude)
        val roundedLon = truncateLatLong(taxiData.longitude)
        val latLonKey = roundedLat.toString + "," + roundedLon.toString
        (latLonKey, (taxiData.latitude, taxiData.longitude, 1))
      })
      .reduceByKeyAndWindow((taxi1: (Float, Float, Int), taxi2: (Float, Float, Int)) => {
        (taxi1._1, taxi1._2, taxi1._3 + taxi2._3)
      }, Seconds(1), Seconds(1))

    aggregatedCount.map(keyPair => {
      s"{'key': '${keyPair._1}', 'lat':${keyPair._2._2}, 'lon':${keyPair._2._2}, 'count':${keyPair._2._3}}"
    })
  }

  def parseDStreamJsonAsTaxiStruct(sparkCtx: SparkSession, dsStreamJson: DStream[String]): Unit = {
    import sparkCtx.implicits._

      /*
    .add("meter_reading", FloatType, nullable = true)
    .add("ride_status", StringType, nullable = true)
    .add("passenger_count", IntegerType, nullable = true)
     */  // not ready

    dsStreamJson.foreachRDD(rdd => {
      val jsonDataFrame = sparkCtx.read.schema(taxiStruct).json(rdd.toDS())
      val filteredNullsDF = jsonDataFrame.where("ride_id is not null")
      filteredNullsDF.foreach(row => {
        println(row.json)
      })
    })
  }

  private def truncateLatLong(coordinate: Float): Float = {
    val PRECISION = 0.005f // very approximately 500m
    (Math.floor(coordinate / PRECISION).toFloat * PRECISION * 10000 + 25f) / 10000f
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
