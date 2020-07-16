package org.myspark.structs

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, TimestampType}

object TaxiStruct {
  val taxiDataSchema: StructType = new StructType()
    .add("ride_id", StringType, nullable = true)
    .add("point_idx", IntegerType, nullable = true)
    .add("latitude", FloatType, nullable = true)
    .add("longitude", FloatType, nullable = true)
    .add("meter_increment", FloatType, nullable = true)
    .add("timestamp", TimestampType, nullable = true)
    .add("ride_status", StringType, nullable = true)
}
