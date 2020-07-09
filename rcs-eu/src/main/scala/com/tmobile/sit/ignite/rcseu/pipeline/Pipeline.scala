package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.rcseu.data.{InputData, PreprocessedData}
import org.apache.spark.sql.SparkSession

class Pipeline(inputData: InputData, stageData: StageProcessing, core: ProcessingCore, writer: ResultWriter)(implicit sparkSession: SparkSession) {
  def run(): Unit = {

    // Read input files
    val inputActivity = inputData.activity.read()
    val inputProvision = inputData.provision.read()
    val inputRegisterRequests = inputData.register_requests.read()

    // Preprocess input files
    val stageActivity = stageData.preprocessActivity(inputActivity)
    val stageProvision = stageData.preprocessProvision(inputProvision)
    val stageRegisterRequests =  stageData.preprocessRegisterRequests(inputRegisterRequests)

    // Create preprocessedData object
    val preprocessedData = PreprocessedData(stageActivity,stageProvision,stageRegisterRequests)

    // Calculate output data from core processing
    val result = core.process(preprocessedData)

    // Write result data set
    writer.write(result)
  }

}
