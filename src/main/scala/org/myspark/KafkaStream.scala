package org.myspark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.myspark.dataTypes.TaxiRide


class KafkaStream(jsonValidator: JsonValidator) extends java.io.Serializable {
  private final val checkPointDir = "C:\\tmp\\hive\\checkpoints"

  def run(): Unit = {
    val streamingContext = StreamingContext.getOrCreate(
      checkPointDir,
      functionToCreateContext _)
    val spark  = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val taxiDataSchema = new StructType()
      .add("ride_id", StringType, nullable = true)
      .add("point_idx", IntegerType, nullable = true)
      .add("latitude", FloatType, nullable = true)
      .add("longitude", FloatType, nullable = true)
      .add("timestamp", TimestampType, nullable = true)
      .add("meter_reading", FloatType, nullable = true)
      .add("meter_increment", FloatType, nullable = true)
      .add("ride_status", StringType, nullable = true)
      .add("passenger_count", IntegerType, nullable = true)

    val filteredOnlyJson = stream
      .filter(record => record.value != null && jsonValidator.isValidRawJson(record.value))
      .map(record => jsonValidator.parse(record.value))
      .filter(jsValue => jsonValidator.isValidStructure[TaxiRide](jsValue))
      .map(jsValue => {
        val taxiData = jsonValidator.toStructure[TaxiRide](jsValue).get
        val roundedLat = truncateLatLong(taxiData.latitude)
        val roundedLon = truncateLatLong(taxiData.longitude)
        val latLonKey = roundedLat.toString + "," + roundedLon.toString
        latLonKey
      })
      .countByValueAndWindow(Seconds(10), Seconds(10))
    filteredOnlyJson.foreachRDD(rdd => {
      rdd.foreach(keyPair => {
        println(s"${keyPair._1} : ${keyPair._2}")
      })
    })
    /*
    filteredOnlyJson.foreachRDD(rdd => {
      val jsonDataFrame = spark.read.schema(taxiDataSchema).json(rdd.toDS())
      val filteredNullsDF = jsonDataFrame.where("ride_id is not null")
      filteredNullsDF.foreach(row => {
        println(row.json)
      })
    })
    */

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def truncateLatLong(coordinate: Float): Float = {
    val PRECISION = 0.005f // very approximately 500m
    (Math.floor(coordinate / PRECISION).toFloat * PRECISION * 10000 + 25f) / 10000f
  }

  private def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName("KakfaConsumer")
      .set("spark.local.dir", "C:\\tmp\\hive")
    //.setMaster("local")   // do not set when using submit-job
    val streamingContext = new StreamingContext(conf, Seconds(1))
    streamingContext.checkpoint(checkPointDir)
    streamingContext
  }
}

object KafkaStream {
  def main(args: Array[String]): Unit = {
    val streamExec = new KafkaStream(Utils)
    streamExec.run()
  }
}
