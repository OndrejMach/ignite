package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
//import com.tmobile.sit.ignite.common.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.common.writers.ParquetWriter
import com.tmobile.sit.ignite.rcseu.data.OutputData
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.rcseu.ParquetApplication.runVar
import com.tmobile.sit.ignite.rcseu.config.Settings

trait Writer extends Logger{
  def write(output: OutputData): Unit
}
/**
 * The ResultWrite class is an implementation of the ParquetWriter over a set of output files
 * required by the RBM pipeline. It takes into consideration the file metadata for the current
 * file date and natco, as well as the ResultsPath class because it's writing both output and
 * lookup files for the next iteration
 */
class ResultWriter(settings: Settings) (implicit sparkSession: SparkSession) extends Writer {
  private def writeWithFixedEmptyDFs(data:DataFrame,path:String): Unit = {
    import sparkSession.implicits._
    data.cache()
//    val cached_data = data.cache()
    if (data.isEmpty){
      print("EMPTY")
      val line = Seq(data.columns.mkString("\t"))
      val df = line.toDF()
      ParquetWriter(df, path, delimiter = "\t", writeHeader = false, quote = "").writeData()
//      CSVWriter(df, path, delimiter = "\t", writeHeader = false, quote = "").writeData()
    } else {
    ParquetWriter(data, path, delimiter = "\t", writeHeader = true).writeData()
//      CSVWriter(data, path, delimiter = "\t", writeHeader = true).writeData()
    }
  }


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
    ParquetWriter(outputData.UserAgents, persistencePath+"User_agents.parquet", delimiter = "\t").writeData()
//    CSVWriter(outputData.UserAgents, persistencePath+"User_agents.csv", delimiter = "\t").writeData()

    // If daily processing or daily update
    if(!runVar.processYearly) {
      if(runVar.runMode.equals("update")) {
        logger.info(s"Updating data for ${runVar.date} by including data from ${runVar.tomorrowDate}")
      }
      else {
        logger.info("Writing daily and monthly data")
      }
      writeWithFixedEmptyDFs(outputData.ActiveDaily, outputPath + "activity_daily." + runVar.natco + "." + runVar.dateforoutput + ".parquet")
      writeWithFixedEmptyDFs(outputData.ActiveMonthly, outputPath + "activity_monthly." + runVar.natco + "." + runVar.monthforoutput + ".parquet")
      writeWithFixedEmptyDFs(outputData.ServiceDaily, outputPath + "service_fact." + runVar.natco + "." + runVar.dateforoutput + ".parquet")
//      writeWithFixedEmptyDFs(outputData.ActiveDaily, outputPath + "activity_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv")
//      writeWithFixedEmptyDFs(outputData.ActiveMonthly, outputPath + "activity_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv")
//      writeWithFixedEmptyDFs(outputData.ServiceDaily, outputPath + "service_fact." + runVar.natco + "." + runVar.dateforoutput + ".csv")

      if(!runVar.runMode.equals("update")) {
        writeWithFixedEmptyDFs(outputData.ProvisionedDaily, outputPath + "provisioned_daily." + runVar.natco + "." + runVar.dateforoutput + ".parquet")
        writeWithFixedEmptyDFs(outputData.RegisteredDaily, outputPath + "registered_daily." + runVar.natco + "." + runVar.dateforoutput + ".parquet")
        writeWithFixedEmptyDFs(outputData.ProvisionedMonthly, outputPath + "provisioned_monthly." + runVar.natco + "." + runVar.monthforoutput + ".parquet")
        writeWithFixedEmptyDFs(outputData.RegisteredMonthly, outputPath + "registered_monthly." + runVar.natco + "." + runVar.monthforoutput + ".parquet")
//        writeWithFixedEmptyDFs(outputData.ProvisionedDaily, outputPath + "provisioned_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv")
//        writeWithFixedEmptyDFs(outputData.RegisteredDaily, outputPath + "registered_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv")
//        writeWithFixedEmptyDFs(outputData.ProvisionedMonthly, outputPath + "provisioned_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv")
//        writeWithFixedEmptyDFs(outputData.RegisteredMonthly, outputPath + "registered_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv")
      }
      else if (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31")) {
        writeWithFixedEmptyDFs(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".parquet")
//        writeWithFixedEmptyDFs(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".csv")
      }
    }

    // if yearly processing
    if(runVar.processYearly) {
      logger.info("Writing yearly data")
      writeWithFixedEmptyDFs(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".parquet")
      writeWithFixedEmptyDFs(outputData.ProvisionedYearly, outputPath + "provisioned_yearly." + runVar.natco + "." + runVar.year + ".parquet")
      writeWithFixedEmptyDFs(outputData.RegisteredYearly, outputPath + "registered_yearly." + runVar.natco + "." + runVar.year + ".parquet")
//      writeWithFixedEmptyDFs(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".csv")
//      writeWithFixedEmptyDFs(outputData.ProvisionedYearly, outputPath + "provisioned_yearly." + runVar.natco + "." + runVar.year + ".csv")
//      writeWithFixedEmptyDFs(outputData.RegisteredYearly, outputPath + "registered_yearly." + runVar.natco + "." + runVar.year + ".csv")
    }
  }
}