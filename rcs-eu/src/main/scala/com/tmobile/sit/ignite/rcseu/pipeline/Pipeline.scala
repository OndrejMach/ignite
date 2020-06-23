package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.config.Settings
import org.apache.spark.sql.SparkSession

class Pipeline(inputData: InputData, stage: StageProcessing, core: ProcessingCore, settings: Settings)(implicit sparkSession: SparkSession) {
  def run(): Unit = {

    // Read input files
    val inputActivity = inputData.activity.read()
    val inputProvision = inputData.provision.read()
    val inputRegisterRequests = inputData.register_requests.read()

    // Preprocess input files
    val preprocessedActivity = stage.preprocessActivity(inputActivity)
    val preprocessedProvision = stage.preprocessProvision(inputProvision)
    val preprocessedRegisterRequest =  stage.preprocessRegisterRequests(inputRegisterRequests)

    // Create preprocessedData object
    val preprocessedData = PreprocessedData(preprocessedActivity,
      preprocessedProvision,preprocessedRegisterRequest)

    // Calculate output data from core processing
    val result = core.process(preprocessedData)

    // Write output
    CSVWriter(data = result,settings.outputPath.get, writeHeader = true)
  }

}
