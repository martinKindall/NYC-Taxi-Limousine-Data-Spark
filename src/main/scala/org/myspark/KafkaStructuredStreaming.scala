package org.myspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._


@SerialVersionUID(6529685098267757694L)
class KafkaStructuredStreaming(taxiStruct: StructType) extends java.io.Serializable {
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

    val query = df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "taxi-sink")
    .option("checkpointLocation", "C:\\tmp\\hive\\checkpoints")
    .start()

    query.awaitTermination()
  }
}
