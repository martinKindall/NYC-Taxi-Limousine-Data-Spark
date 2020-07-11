package org.myspark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.myspark.dataTypes.TaxiRide


class KafkaStream(jsonValidator: JsonValidator) extends java.io.Serializable {
  private final val checkPointDir = "C:\\tmp\\hive\\checkpoints"
  private val sparkCtx: SparkSession  = SparkSession.builder()
    .getOrCreate()
  import sparkCtx.implicits._

  def run(): Unit = {
    val streamingContext = StreamingContext.getOrCreate(
      checkPointDir,
      functionToCreateContext _)

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

    val filteredOnlyJson = stream
      .filter(record => record.value != null && jsonValidator.isValidRawJson(record.value))
      .map(record => jsonValidator.parse(record.value))
      .filter(jsValue => jsonValidator.isValidStructure[TaxiRide](jsValue))

    parseDStreamJsonAggregateAndWriteToKafka(
      filteredOnlyJson.map(jsValue => {
        jsonValidator.toStructure[TaxiRide](jsValue).get
    }))

    parseDStreamJsonAsTaxiStruct(filteredOnlyJson.map(jsValue => jsValue.toString()))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def truncateLatLong(coordinate: Float): Float = {
    val PRECISION = 0.005f // very approximately 500m
    (Math.floor(coordinate / PRECISION).toFloat * PRECISION * 10000 + 25f) / 10000f
  }

  private def functionToCreateContext(): StreamingContext = {
    sparkCtx.sparkContext.getConf
      .setAppName("KafkaConsumer")
      .set("spark.local.dir", "C:\\tmp\\hive")
    //.setMaster("local")   // do not set when using submit-job
    val streamingContext = new StreamingContext(sparkCtx.sparkContext, Seconds(1))
    streamingContext.checkpoint(checkPointDir)
    streamingContext
  }

  private def parseDStreamJsonAsTaxiStruct(dsStreamJson: DStream[String]): Unit = {
    val taxiDataSchema = new StructType()
      .add("ride_id", StringType, nullable = true)
      .add("point_idx", IntegerType, nullable = true)
      .add("latitude", FloatType, nullable = true)
      .add("longitude", FloatType, nullable = true)
      /*.add("timestamp", TimestampType, nullable = true)
      .add("meter_reading", FloatType, nullable = true)
      .add("meter_increment", FloatType, nullable = true)
      .add("ride_status", StringType, nullable = true)
      .add("passenger_count", IntegerType, nullable = true)
       */  // not ready

    dsStreamJson.foreachRDD(rdd => {
      val jsonDataFrame = sparkCtx.read.schema(taxiDataSchema).json(rdd.toDS())
      val filteredNullsDF = jsonDataFrame.where("ride_id is not null")
      filteredNullsDF.foreach(row => {
        println(row.json)
      })
    })
  }

  private def parseDStreamJsonAggregateAndWriteToKafka(filteredOnlyJson: DStream[TaxiRide]): Unit = {
    val aggregatedCount = filteredOnlyJson
      .map(taxiData => {
        val roundedLat = truncateLatLong(taxiData.latitude)
        val roundedLon = truncateLatLong(taxiData.longitude)
        val latLonKey = roundedLat.toString + "," + roundedLon.toString
        (latLonKey, (taxiData.latitude, taxiData.longitude, 1))
      })
      .reduceByKeyAndWindow((taxi1: (Float, Float, Int), taxi2: (Float, Float, Int)) => {
        (taxi1._1, taxi1._2, taxi1._3 + taxi2._3)
      }, Seconds(1), Seconds(1))

    aggregatedCount.foreachRDD(rdd => {
      val formattedRDD = rdd.map(keyPair => {
        s"{'key': '${keyPair._1}', 'lat':${keyPair._2._2}, 'lon':${keyPair._2._2}, 'count':${keyPair._2._3}}"
      })
      formattedRDD
        .toDF("value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "taxi-data")
        .save()
    })
  }
}

object KafkaStream {
  def main(args: Array[String]): Unit = {
    val streamExec = new KafkaStream(Utils)
    streamExec.run()
  }
}
