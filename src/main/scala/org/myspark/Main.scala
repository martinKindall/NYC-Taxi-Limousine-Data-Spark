package org.myspark

object Main {
  def main(args: Array[String]): Unit = {
    //val streamExec = new KafkaStreaming(Utils, new TaxiOperations)
    val streamExec = new KafkaStructuredStreaming
    streamExec.run()
  }
}
