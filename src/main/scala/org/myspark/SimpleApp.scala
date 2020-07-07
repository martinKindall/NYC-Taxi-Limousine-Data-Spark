package org.myspark
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "C:\\spark-3.0.0-bin-hadoop2.7\\README.md" // Should be some file on your system
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.local.dir", "C:\\tmp\\hive")
      .getOrCreate()

    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}