package org.myspark.operations

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.myspark.dataTypes.TaxiRide


@SerialVersionUID(6529685098267757690L)
class TaxiOperations(taxiStruct: StructType) extends java.io.Serializable {

  def parseDStreamTaxiSumIncrements(dsTaxiStream: DStream[TaxiRide]): DStream[String] = {
    dsTaxiStream
      .map(taxiData => {
        taxiData.meterIncrement
      })
      .reduceByWindow((amount1: Float, amount2: Float) => amount1 + amount2,
        Seconds(60), Seconds(3))
      .map(totalSum => s"{'dollar_per_minute': $totalSum}")
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
