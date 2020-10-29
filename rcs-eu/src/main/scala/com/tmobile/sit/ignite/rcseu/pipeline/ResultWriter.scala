package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.data.{OutputData, ResultPaths}
import com.tmobile.sit.ignite.rcseu.data.{OutputData, ResultPaths}
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.ignite.rcseu.Application.date
import com.tmobile.sit.ignite.rcseu.Application.month
import com.tmobile.sit.ignite.rcseu.Application.year
import com.tmobile.sit.ignite.rcseu.Application.natco
import com.tmobile.sit.ignite.rcseu.Application.isHistoric
import com.tmobile.sit.ignite.rcseu.Application.dateforoutput
import com.tmobile.sit.ignite.rcseu.Application.monthforoutput




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

    //if isHistoric = true (if the config parameter is true)
    if(isHistoric) {
      CSVWriter(outputData.UserAgents, resultPaths.outputPath+"User_Agents.csv", delimiter = ";").writeData()

      outputData.AccActivity.write.mode("overwrite").parquet(resultPaths.lookupPath+"acc_activity.parquet")
      outputData.AccProvision.write.mode("overwrite").parquet(resultPaths.lookupPath+"acc_provision.parquet")
      outputData.AccRegisterRequests.write.mode("overwrite").parquet(resultPaths.lookupPath+"acc_register_requests.parquet")

    }
    else {
      CSVWriter(outputData.UserAgents, resultPaths.outputPath + "User_Agents.csv", delimiter = ";").writeData()

      CSVWriter(outputData.ProvisionedDaily, resultPaths.outputPath + "provisioned_daily." + natco + "." + dateforoutput + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.ProvisionedMonthly, resultPaths.outputPath + "provisioned_monthly." + natco + "." + monthforoutput + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.ProvisionedYearly, resultPaths.outputPath + "provisioned_yearly." + natco + "." + year + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.ProvisionedTotal, resultPaths.outputPath + "provisioned_total." + natco + ".csv", delimiter = ";").writeData()

      CSVWriter(outputData.RegisteredDaily, resultPaths.outputPath + "registered_daily." + natco + "." + dateforoutput + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.RegisteredMonthly, resultPaths.outputPath + "registered_monthly." + natco + "." + monthforoutput + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.RegisteredYearly, resultPaths.outputPath + "registered_yearly." + natco + "." + year + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.RegisteredTotal, resultPaths.outputPath + "registered_total." + natco + ".csv", delimiter = ";").writeData()

      CSVWriter(outputData.ActiveDaily, resultPaths.outputPath + "activity_daily." + natco + "." + dateforoutput + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.ActiveMonthly, resultPaths.outputPath + "activity_monthly." + natco + "." + monthforoutput + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.ActiveYearly, resultPaths.outputPath + "activity_yearly." + natco + "." + year + ".csv", delimiter = ";").writeData()
      CSVWriter(outputData.ActiveTotal, resultPaths.outputPath + "activity_total." + natco + ".csv", delimiter = ";").writeData()

      CSVWriter(outputData.ServiceDaily, resultPaths.outputPath + "service_fact." + natco + "." + dateforoutput + ".csv", delimiter = ";").writeData()

      outputData.AccActivity.write.mode("overwrite").parquet(resultPaths.lookupPath + "acc_activity.parquet")
      outputData.AccProvision.write.mode("overwrite").parquet(resultPaths.lookupPath + "acc_provision.parquet")
      outputData.AccRegisterRequests.write.mode("overwrite").parquet(resultPaths.lookupPath + "acc_register_requests.parquet")
    }
    //CSVWriter(outputData.AccActivity, resultPaths.lookupPath+"acc_activity.csv", delimiter = ";").writeData()
    //CSVWriter(outputData.AccProvision, resultPaths.lookupPath+"acc_provision.csv", delimiter = ";").writeData()
    //CSVWriter(outputData.AccRegisterRequests, resultPaths.lookupPath+"acc_register_requests.csv", delimiter = ";").writeData()

  }
}