package org.myspark

import org.apache.spark.sql.SparkSession

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
