package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.hotspot.config.{InputConfig, StageConfig}
import com.tmobile.sit.ignite.hotspot.readers.TextReader
import org.apache.spark.sql.SparkSession

class OrderDBInputData(stageConfig: StageConfig, inputConfig: InputConfig)(implicit  sparkSession: SparkSession) {
  val dataHotspot = sparkSession.read.parquet(stageConfig.wlan_hotspot_filename.get).cache()

  val inputMPS = new TextReader(inputConfig.MPS_filename.get).read()

  val oldErrorCodes = //sparkSession.read.parquet(stageConfig.error_codes_filename.get).persist()


    CSVReader(schema = Some(OrderDBStructures.errorCodesStructure),
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      path = stageConfig.error_codes_filename.get,
      header = true,
      delimiter = "|")
    .read()


}

object OrderDBInputData {
  def apply(stageConfig: StageConfig, inputConfig: InputConfig)(implicit sparkSession: SparkSession): OrderDBInputData = new OrderDBInputData(stageConfig, inputConfig: InputConfig)
}