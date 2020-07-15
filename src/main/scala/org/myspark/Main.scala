package org.myspark

import org.myspark.structs.TaxiStruct

object Main {
  def main(args: Array[String]): Unit = {
    //val streamExec = new KafkaStreaming(Utils, new TaxiOperations(TaxiStruct.taxiDataSchema))
    val streamExec = new KafkaStructuredStreaming(TaxiStruct.taxiDataSchema)
    streamExec.run()
  }
}
