package org.myspark.operations

import org.apache.spark.sql.functions.window
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
}
