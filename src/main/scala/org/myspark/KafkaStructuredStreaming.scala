package org.myspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.myspark.operations.TaxiStructuredOperations


@SerialVersionUID(6529685098267757694L)
class KafkaStructuredStreaming(taxiStruct: StructType, taxiOperations: TaxiStructuredOperations) extends java.io.Serializable {
  private val sparkCtx: SparkSession  = SparkSession.builder()
    .getOrCreate()

  def run(): Unit = {
    val df = sparkCtx
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()

    val query= df.select(
        col("key").cast("string"),
        from_json(
          col("value").cast("string"),
          taxiStruct).alias("taxi_ride")
      ).select("taxi_ride.*")

    /*
    val finalQuery = taxiOperations.toSumIncrementsEventTime(sparkCtx, query)
      .toJSON
      .toDF("value")
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
     */

    val finalQuery = taxiOperations.toSessionWindowPerRide(sparkCtx, query)
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    finalQuery.awaitTermination()
  }
}
