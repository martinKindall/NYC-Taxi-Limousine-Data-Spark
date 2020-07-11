package org.myspark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.myspark.dataTypes.TaxiRide


class ProcessKafkaStream(jsonValidator: JsonValidator, taxiOperations: TaxiOperations) extends java.io.Serializable {
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

    val structuredTaxiStream = filteredOnlyJson.map(jsValue => {
      jsonValidator.toStructure[TaxiRide](jsValue).get
    })

    taxiOperations
      .parseDStreamJsonCountRides(structuredTaxiStream)
      .foreachRDD(rdd => {
        rdd
          .toDF("value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "taxi-data")
          .save()
      })

    taxiOperations
      .parseDStreamJsonSumIncrements(structuredTaxiStream)
      .foreachRDD(rdd => {
        rdd
          .toDF("value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "taxi-dolar")
          .save()
      })

    taxiOperations.parseDStreamJsonAsTaxiStruct(
      sparkCtx,
      filteredOnlyJson.map(jsValue => jsValue.toString()))

    streamingContext.start()
    streamingContext.awaitTermination()
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
}

object ProcessKafkaStream {
  def main(args: Array[String]): Unit = {
    val streamExec = new ProcessKafkaStream(Utils, new TaxiOperations)
    streamExec.run()
  }
}
