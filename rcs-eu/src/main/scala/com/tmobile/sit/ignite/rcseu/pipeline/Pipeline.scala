package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.rcseu.data.{InputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.SparkSession

class Pipeline(inputData: InputData, persistentData: PersistentData, stageData: StageProcessing,
               core: ProcessingCore, writer: ResultWriter)(implicit sparkSession: SparkSession) {
  def run(): Unit = {

    // Read input files
    val inputActivity = inputData.activity.read()
    val inputProvision = inputData.provision.read()
    val inputRegisterRequests = inputData.register_requests.read()

    // Preprocess input files
    val stageActivityAcc = stageData.preprocessActivity(inputActivity,persistentData.accumulated_activity)
    val stageProvisionAcc = stageData.preprocessProvision(inputProvision,persistentData.accumulated_provision)
    val stageRegisterRequestsAcc =  stageData.preprocessRegisterRequests(inputRegisterRequests,persistentData.accumulated_register_requests)

    // Create preprocessedData object
    val preprocessedData = PreprocessedData(stageActivityAcc,stageProvisionAcc,stageRegisterRequestsAcc)

    // Calculate output data from core processing
    val result = core.process(preprocessedData, persistentData)

    // Write result data set
    writer.write(result)
  }

}
