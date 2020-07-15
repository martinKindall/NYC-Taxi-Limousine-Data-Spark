package org.myspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class KafkaStructuredStreaming {
  private val sparkCtx: SparkSession  = SparkSession.builder()
    .getOrCreate()
  import sparkCtx.implicits._

  def run(): Unit = {
    val df = sparkCtx
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("auto.offset.reset", "earliest")
      .load()

    val schema = new StructType()
      .add("ride_id", StringType, nullable = true)
      .add("ride_id", StringType, nullable = true)


    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = df
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
