package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.data.{OutputData}
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.ignite.rcseu.Application.runVar
import com.tmobile.sit.ignite.rcseu.config.Settings

trait Writer extends Logger{
  def write(output: OutputData): Unit
}
/**
 * The ResultWrite class is an implementation of the CSVWriter over a set of output files
 * required by the RBM pipeline. It takes into consideration the file metadata for the current
 * file date and natco, as well as the ResultsPath class because it's writing both output and
 * lookup files for the next iteration
 */
class ResultWriter(settings: Settings) (implicit sparkSession: SparkSession) extends Writer {

  override def write(outputData: OutputData) =
  {
    logger.info("Writing output files")

    val persistencePath = settings.lookupPath.get
    val outputPath = settings.outputPath.get

    /*
    if(runVar.debug) {
      logger.info(s"Writing acc_activity_${runVar.natco}.parquet")
      outputData.AccActivity.write.mode("overwrite").parquet(persistencePath + s"acc_activity_${runVar.natco}.parquet")
      logger.info(s"Writing acc_provision_${runVar.natco}.parquet")
      outputData.AccProvision.write.mode("overwrite").parquet(persistencePath + s"acc_provision_${runVar.natco}.parquet")
      logger.info(s"Writing acc_register_requests.parquet")
      outputData.AccRegisterRequests.write.mode("overwrite").parquet(persistencePath + s"acc_register_requests_${runVar.natco}.parquet")
    }*/

    // Always write user_agents
    CSVWriter(outputData.UserAgents, persistencePath+"User_agents.csv", delimiter = "\t").writeData()

    // If daily processing or daily update
    if(!runVar.processYearly) {
      if(runVar.runMode.equals("update")) {
        logger.info(s"Updating data for ${runVar.date} by including data from ${runVar.tomorrowDate}")
      }
      else {
        logger.info("Writing daily and monthly data")
      }
      CSVWriter(outputData.ActiveDaily, outputPath + "activity_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ActiveMonthly, outputPath + "activity_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ServiceDaily, outputPath + "service_fact." + runVar.natco + "." + runVar.dateforoutput + ".csv", delimiter = "\t").writeData()

      if(!runVar.runMode.equals("update")) {
        CSVWriter(outputData.ProvisionedDaily, outputPath + "provisioned_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv", delimiter = "\t").writeData()
        CSVWriter(outputData.RegisteredDaily, outputPath + "registered_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv", delimiter = "\t").writeData()
        CSVWriter(outputData.ProvisionedMonthly, outputPath + "provisioned_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv", delimiter = "\t").writeData()
        CSVWriter(outputData.RegisteredMonthly, outputPath + "registered_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv", delimiter = "\t").writeData()
      }
    }

    // if yearly processing
    if(runVar.processYearly) {
      logger.info("Writing yearly data")
      CSVWriter(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ProvisionedYearly, outputPath + "provisioned_yearly." + runVar.natco + "." + runVar.year + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.RegisteredYearly, outputPath + "registered_yearly." + runVar.natco + "." + runVar.year + ".csv", delimiter = "\t").writeData()
    }
  }
}