package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.data.{OutputData}
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.ignite.rcseu.Application.date
import com.tmobile.sit.ignite.rcseu.Application.month
import com.tmobile.sit.ignite.rcseu.Application.year
import com.tmobile.sit.ignite.rcseu.Application.natco
import com.tmobile.sit.ignite.rcseu.Application.isHistoric
import com.tmobile.sit.ignite.rcseu.Application.dateforoutput
import com.tmobile.sit.ignite.rcseu.Application.monthforoutput
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
    val debug = false;

    logger.info("Writing output files")

    val persistencePath = settings.lookupPath.get
    val outputPath = settings.outputPath.get

    // Write the accumulators and dimension always
    CSVWriter(outputData.UserAgents, persistencePath+"User_agents.csv", delimiter = "\t").writeData()

    if(debug) {
      logger.info(s"Writing acc_activity_${natco}.parquet")
      outputData.AccActivity.write.mode("overwrite").parquet(persistencePath + s"acc_activity_${natco}.parquet")
      logger.info(s"Writing acc_provision_${natco}.parquet")
      outputData.AccProvision.write.mode("overwrite").parquet(persistencePath + s"acc_provision_${natco}.parquet")
      logger.info(s"Writing acc_register_requests.parquet")
      outputData.AccRegisterRequests.write.mode("overwrite").parquet(persistencePath + s"acc_register_requests_${natco}.parquet")
    }

    //if isHistoric = false, write all files
    if(!isHistoric) {

      CSVWriter(outputData.ProvisionedDaily, outputPath + "provisioned_daily." + natco + "." + dateforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ProvisionedMonthly, outputPath + "provisioned_monthly." + natco + "." + monthforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ProvisionedYearly, outputPath + "provisioned_yearly." + natco + "." + year + ".csv", delimiter = "\t").writeData()

      CSVWriter(outputData.RegisteredDaily, outputPath + "registered_daily." + natco + "." + dateforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.RegisteredMonthly, outputPath + "registered_monthly." + natco + "." + monthforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.RegisteredYearly, outputPath + "registered_yearly." + natco + "." + year + ".csv", delimiter = "\t").writeData()

      CSVWriter(outputData.ActiveDaily, outputPath + "activity_daily." + natco + "." + dateforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ActiveMonthly, outputPath + "activity_monthly." + natco + "." + monthforoutput + ".csv", delimiter = "\t").writeData()
      CSVWriter(outputData.ActiveYearly, outputPath + "activity_yearly." + natco + "." + year + ".csv", delimiter = "\t").writeData()

      CSVWriter(outputData.ServiceDaily, outputPath + "service_fact." + natco + "." + dateforoutput + ".csv", delimiter = "\t").writeData()
    }
  }
}