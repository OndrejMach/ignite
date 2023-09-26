package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.RCSEUParquetReader
import com.tmobile.sit.ignite.rcseu.Application.settings
import org.apache.spark.sql.functions.lit
import com.tmobile.sit.ignite.common.common.writers.RCSEUParquetWriter
import com.tmobile.sit.ignite.rcseu.data.OutputData
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.rcseu.Application.runVar
import com.tmobile.sit.ignite.rcseu.config.Settings

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

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
  private def writeWithFixedEmptyDFs(data:DataFrame,path:String, writeMode:String, partitionCols:Seq[String]): Unit = {
    import sparkSession.implicits._
    data.cache()
    if (data.isEmpty){
      print("EMPTY")
      val line = Seq(data.columns.mkString("\t"))
      val df = line.toDF()
      RCSEUParquetWriter(df, path).writeParquetData(writeMode = writeMode, partitionCols = partitionCols)
    } else {
      RCSEUParquetWriter(data, path).writeParquetData(writeMode = writeMode, partitionCols = partitionCols)
    }
  }

  def addDailyPartitioning(data: DataFrame): DataFrame = {
    data
      .withColumn("natco", lit(runVar.natco))
      .withColumn("year", lit(runVar.year))
      .withColumn("month", lit(runVar.monthNum))
      .withColumn("day", lit(runVar.dayNum))
  }

  def addMontlyPartitioning(data: DataFrame): DataFrame = {
    data
      .withColumn("natco", lit(runVar.natco))
      .withColumn("year", lit(runVar.year))
      .withColumn("month", lit(runVar.monthNum))
  }

  def addYearlyPartitioning(data: DataFrame): DataFrame = {
    data
      .withColumn("natco", lit(runVar.natco))
      .withColumn("year", lit(runVar.year))
  }

  override def write(outputData: OutputData) =
  {
    logger.info("Writing output files")

    val persistencePath = settings.lookupPath.get
    val outputPath = settings.outputPath.get

    // If daily processing or daily update
    if(!runVar.processYearly) {
      if(runVar.runMode.equals("update")) {
        logger.info(s"Updating data for ${runVar.date} by including data from ${runVar.tomorrowDate}")
      }
      else {
        logger.info("Writing daily and monthly data")
      }
      writeWithFixedEmptyDFs(addDailyPartitioning(outputData.ActiveDaily), outputPath + "activity_daily/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))
      writeWithFixedEmptyDFs(addMontlyPartitioning(outputData.ActiveMonthly), outputPath + "activity_monthly/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month"))
      writeWithFixedEmptyDFs(addDailyPartitioning(outputData.ServiceDaily), outputPath + "service_fact/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))

      if(!runVar.runMode.equals("update")) {
        writeWithFixedEmptyDFs(addDailyPartitioning(outputData.ProvisionedDaily), outputPath + "provisioned_daily/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))
        writeWithFixedEmptyDFs(addDailyPartitioning(outputData.RegisteredDaily), outputPath + "registered_daily/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))
        writeWithFixedEmptyDFs(addMontlyPartitioning(outputData.ProvisionedMonthly), outputPath + "provisioned_monthly/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month"))
        writeWithFixedEmptyDFs(addMontlyPartitioning(outputData.RegisteredMonthly), outputPath + "registered_monthly/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month"))
      }
      else if (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31")) {
        writeWithFixedEmptyDFs(addYearlyPartitioning(outputData.ActiveYearly), outputPath + "activity_yearly/", writeMode = "overwrite", partitionCols = Seq("natco", "year"))
      }
    }

    // if yearly processing
    if(runVar.processYearly) {
      logger.info("Writing yearly data")
      writeWithFixedEmptyDFs(addYearlyPartitioning(outputData.ActiveYearly), outputPath + "activity_yearly/", writeMode = "overwrite", Seq("natco", "year"))
      writeWithFixedEmptyDFs(addYearlyPartitioning(outputData.ProvisionedYearly), outputPath + "provisioned_yearly/", writeMode = "overwrite", Seq("natco", "year"))
      writeWithFixedEmptyDFs(addYearlyPartitioning(outputData.RegisteredYearly), outputPath + "registered_yearly/", writeMode = "overwrite", Seq("natco", "year"))
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