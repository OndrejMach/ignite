package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.spark.sql.SparkSession

class Pipeline(inputData: InputData, stage: StageProcessing, core: ProcessingCore, settings: Settings)(implicit sparkSession: SparkSession) {
  def run(): Unit = {

    val preprocessedData =
      PreprocessedData(stage.preprocessActivity(inputData.activity.read()),
                       stage.preprocessProvision(inputData.provision.read()),
                       stage.preprocessRegisterRequests(inputData.register_requests.read())
      )

    val result = core.process(preprocessedData)


    CSVWriter(data = result,settings.outputPath.get, writeHeader = true)
  }

}
