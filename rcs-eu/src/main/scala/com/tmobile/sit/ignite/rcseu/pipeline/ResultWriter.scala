package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.data.{OutputData, ResultPaths}
import com.tmobile.sit.ignite.rcseu.data.{OutputData, ResultPaths}
import org.apache.spark.sql.SparkSession

trait Writer extends Logger{
  def write(output: OutputData): Unit
}
/**
 * The ResultWrite class is an implementation of the CSVWriter over a set of output files
 * required by the RBM pipeline. It takes into consideration the file metadata for the current
 * file date and natco, as well as the ResultsPath class because it's writing both output and
 * lookup files for the next iteration
 */
class ResultWriter(resultPaths: ResultPaths) (implicit sparkSession: SparkSession) extends Writer {

  override def write(outputData: OutputData) =
  {
    logger.info("Writing output files")

    //val fileSuffix = fileMetaData.file_date.replace("-","")+"_"+fileMetaData.file_natco_id

    CSVWriter(outputData.UserAgents, resultPaths.outputPath+"UserAgents.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ProvisionedDaily, resultPaths.outputPath+"ProvisionedDaily.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ProvisionedMonthly, resultPaths.outputPath+"ProvisionedMonthly.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ProvisionedYearly, resultPaths.outputPath+"ProvisionedYearly.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ProvisionedTotal, resultPaths.outputPath+"ProvisionedTotal.csv", delimiter = ";").writeData()

    CSVWriter(outputData.RegisteredDaily, resultPaths.outputPath+"RegisteredDaily.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ActiveDaily, resultPaths.outputPath+"ActiveDaily.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ServiceDaily, resultPaths.outputPath+"ServiceFactsDaily.csv", delimiter = ";").writeData()
    CSVWriter(outputData.AccActivity, resultPaths.lookupPath+"acc_activity.csv", delimiter = ";").writeData()
    CSVWriter(outputData.AccProvision, resultPaths.lookupPath+"acc_provision.csv", delimiter = ";").writeData()
    CSVWriter(outputData.AccRegisterRequests, resultPaths.lookupPath+"acc_register_requests.csv", delimiter = ";").writeData()

    //TODO: add result path and writer, same as above
  }
}