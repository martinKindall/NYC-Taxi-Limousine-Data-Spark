package org.myspark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

class KafkaStream(private val jsonValidator: JsonValidator) extends java.io.Serializable {
  def run = {
    val conf = new SparkConf()
      .setAppName("KakfaConsumer")
      .set("spark.local.dir", "C:\\tmp\\hive")
    //.setMaster("local")   // do not set when using submit-job
    val streamingContext = new StreamingContext(conf, Seconds(1))
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

    val filteredOnlyJson = stream
      .filter(record => jsonValidator.isValidRawJson(record.value))
      .map(record => record.value)
    filteredOnlyJson.foreachRDD(rdd => {
      val jsonDataFrame = spark.read.json(rdd.toDS())
      jsonDataFrame.printSchema()
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

object KafkaStream {
  def main(args: Array[String]): Unit = {
    val streamExec = new KafkaStream(Utils)
    streamExec.run
  }
}
