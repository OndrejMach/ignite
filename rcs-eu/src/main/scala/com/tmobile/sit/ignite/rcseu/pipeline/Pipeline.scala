package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.{count, desc}
import com.tmobile.sit.ignite.rcseu.data.{InputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.ignite.rcseu.Application.runVar


class Pipeline(inputData: InputData, persistentData: PersistentData, stage: StageProcessing,
               core: ProcessingCore, writer: ResultWriter)(implicit sparkSession: SparkSession) extends Logger{
  def run(): Unit = {

    // Read input files
    val inputActivity = inputData.activity
    val inputProvision = inputData.provision
    val inputRegisterRequests = inputData.register_requests

    if(runVar.debug) {
    logger.info("Inputs")
    inputActivity.agg(count("*").as("no_records")).show(3)
    inputProvision.agg(count("*").as("no_records")).show(3)
    inputRegisterRequests.agg(count("*").as("no_records")).show(3)
    }

    // Read archive files, extract and add file date
    val archiveActivity = stage.preprocessAccumulator(persistentData.activity_archives)
    val archiveProvision = stage.preprocessAccumulator(persistentData.provision_archives)
    val archiveRegisterRequests = stage.preprocessAccumulator(persistentData.register_requests_archives)

    if(runVar.debug) {
    logger.info("Archives")
    archiveActivity.groupBy("FileDate").agg(count("*").as("no_records")).orderBy(desc("FileDate")).show(3)
    archiveProvision.groupBy("FileDate").agg(count("*").as("no_records")).orderBy(desc("FileDate")).show(3)
    archiveRegisterRequests.groupBy("FileDate").agg(count("*").as("no_records")).orderBy(desc("FileDate")).show(3)
    }

    // Get accumulators (archive + input)
    val accActivity = stage.accumulateActivity(inputActivity,archiveActivity)
    val accProvision = stage.accumulateProvision(inputProvision,archiveProvision)
    val accRegisterRequests =  stage.accumulateRegisterRequests(inputRegisterRequests,archiveRegisterRequests)

    if(runVar.debug) {
    logger.info("Accumulated")
    accActivity.groupBy("FileDate").agg(count("*").as("no_records")).orderBy(desc("FileDate")).show(3)
    accProvision.groupBy("FileDate").agg(count("*").as("no_records")).orderBy(desc("FileDate")).show(3)
    accRegisterRequests.groupBy("FileDate").agg(count("*").as("no_records")).orderBy(desc("FileDate")).show(3)
    }

    // Create preprocessedData object
    val preprocessedData = PreprocessedData(accActivity,accProvision,accRegisterRequests)

    // Calculate output data from core processing
    val result = core.process(inputData,preprocessedData,persistentData)

    // Write result data set
    writer.write(result)
  }

}
