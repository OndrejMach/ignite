package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.RCSEUParquetReader
import org.apache.spark.sql.functions.lit
import com.tmobile.sit.ignite.common.common.writers.{RCSEUParquetWriter, RCSEUCSVWriter}
import com.tmobile.sit.ignite.rcseu.data.OutputData
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.rcseu.Application.runVar
import com.tmobile.sit.ignite.rcseu.config.Settings

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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
    if (data.isEmpty){
      val line = Seq(data.columns.mkString("\t"))
      val df = line.toDF()
      RCSEUCSVWriter(df, path, delimiter = "\t", writeHeader = false, quote = "").writeData()
    } else {
      RCSEUCSVWriter(data, path, delimiter = "\t", writeHeader = true).writeData()
    }
  }


  override def write(outputData: OutputData) =
  {
    logger.info("Writing output files")

    val persistencePath = settings.lookupPath.get
    val outputPath = settings.outputPath.get

    // If daily processing or daily update

    // If daily processing or daily update
    if (!runVar.processYearly) {
      if (runVar.runMode.equals("update")) {
        logger.info(s"Updating data for ${runVar.date} by including data from ${runVar.tomorrowDate}")
      }
      else {
        logger.info("Writing daily and monthly data")
      }
      writeWithFixedEmptyDFs(outputData.ActiveDaily, outputPath + "activity_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv")
      writeWithFixedEmptyDFs(outputData.ActiveMonthly, outputPath + "activity_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv")
      writeWithFixedEmptyDFs(outputData.ServiceDaily, outputPath + "service_fact." + runVar.natco + "." + runVar.dateforoutput + ".csv")

      if (!runVar.runMode.equals("update")) {
        writeWithFixedEmptyDFs(outputData.ProvisionedDaily, outputPath + "provisioned_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv")
        writeWithFixedEmptyDFs(outputData.RegisteredDaily, outputPath + "registered_daily." + runVar.natco + "." + runVar.dateforoutput + ".csv")
        writeWithFixedEmptyDFs(outputData.ProvisionedMonthly, outputPath + "provisioned_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv")
        writeWithFixedEmptyDFs(outputData.RegisteredMonthly, outputPath + "registered_monthly." + runVar.natco + "." + runVar.monthforoutput + ".csv")
      }
      else if (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31")) {
        writeWithFixedEmptyDFs(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".csv")
      }
    }

    // if yearly processing
    if (runVar.processYearly) {
      logger.info("Writing yearly data")
      writeWithFixedEmptyDFs(outputData.ActiveYearly, outputPath + "activity_yearly." + runVar.natco + "." + runVar.year + ".csv")
      writeWithFixedEmptyDFs(outputData.ProvisionedYearly, outputPath + "provisioned_yearly." + runVar.natco + "." + runVar.year + ".csv")
      writeWithFixedEmptyDFs(outputData.RegisteredYearly, outputPath + "registered_yearly." + runVar.natco + "." + runVar.year + ".csv")
    }
    // Always write user_agents
    RCSEUParquetWriter(outputData.UserAgents, persistencePath + "User_agents_tmp.parquet").writeParquetData(writeMode = "overwrite", null)
    val newUserAgents = new RCSEUParquetReader(persistencePath + "User_agents_tmp.parquet", persistencePath + "User_agents_tmp.parquet").read()
    RCSEUParquetWriter(newUserAgents, persistencePath + "User_agents.parquet").writeParquetData(writeMode = "overwrite", null)

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    hdfs.delete(new Path(persistencePath + "User_agents_tmp.parquet"), true)
  }
}