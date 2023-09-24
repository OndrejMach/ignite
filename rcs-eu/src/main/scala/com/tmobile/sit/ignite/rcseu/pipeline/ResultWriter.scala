package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.ParquetReader
import com.tmobile.sit.ignite.rcseu.Application.settings
import org.apache.spark.sql.functions.lit
//import com.tmobile.sit.ignite.common.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.common.writers.ParquetWriter
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
      ParquetWriter(df, path).writeParquetData(writeMode = writeMode, partitionBy = true, partitionCols = partitionCols)
    } else {
    ParquetWriter(data, path).writeParquetData(writeMode = writeMode, partitionBy = true, partitionCols = partitionCols)
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

    // If daily processing or daily update
    if(!runVar.processYearly) {
      if(runVar.runMode.equals("update")) {
        logger.info(s"Updating data for ${runVar.date} by including data from ${runVar.tomorrowDate}")
      }
      else {
        logger.info("Writing daily and monthly data")
      }
      writeWithFixedEmptyDFs(outputData.ActiveDaily
        .withColumn("natco", lit(runVar.natco))
        .withColumn("year", lit(runVar.year))
        .withColumn("month", lit(runVar.monthNum))
        .withColumn("day", lit(runVar.dayNum)), outputPath + "activity_daily/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))
      writeWithFixedEmptyDFs(outputData.ActiveMonthly
        .withColumn("natco", lit(runVar.natco))
        .withColumn("year", lit(runVar.year))
        .withColumn("month", lit(runVar.monthNum)), outputPath + "activity_monthly/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month"))
      writeWithFixedEmptyDFs(outputData.ServiceDaily
        .withColumn("natco", lit(runVar.natco))
        .withColumn("year", lit(runVar.year))
        .withColumn("month", lit(runVar.monthNum))
        .withColumn("day", lit(runVar.dayNum)), outputPath + "service_fact/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))

      if(!runVar.runMode.equals("update")) {
        writeWithFixedEmptyDFs(outputData.ProvisionedDaily
          .withColumn("natco", lit(runVar.natco))
          .withColumn("year", lit(runVar.year))
          .withColumn("month", lit(runVar.monthNum))
          .withColumn("day", lit(runVar.dayNum)), outputPath + "provisioned_daily/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))
        writeWithFixedEmptyDFs(outputData.RegisteredDaily
          .withColumn("natco", lit(runVar.natco))
          .withColumn("year", lit(runVar.year))
          .withColumn("month", lit(runVar.monthNum))
          .withColumn("day", lit(runVar.dayNum)), outputPath + "registered_daily/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month", "day"))
        writeWithFixedEmptyDFs(outputData.ProvisionedMonthly
          .withColumn("natco", lit(runVar.natco))
          .withColumn("year", lit(runVar.year))
          .withColumn("month", lit(runVar.monthNum)), outputPath + "provisioned_monthly/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month"))
        writeWithFixedEmptyDFs(outputData.RegisteredMonthly
          .withColumn("natco", lit(runVar.natco))
          .withColumn("year", lit(runVar.year))
          .withColumn("month", lit(runVar.monthNum)), outputPath + "registered_monthly/", writeMode = "overwrite", partitionCols = Seq("natco", "year", "month"))
      }
      else if (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31")) {
        writeWithFixedEmptyDFs(outputData.ActiveYearly
          .withColumn("natco", lit(runVar.natco))
          .withColumn("year", lit(runVar.year)), outputPath + "activity_yearly/", writeMode = "overwrite", partitionCols = Seq("natco", "year"))
      }
    }

    // if yearly processing
    if(runVar.processYearly) {
      logger.info("Writing yearly data")
      writeWithFixedEmptyDFs(outputData.ActiveYearly
        .withColumn("natco", lit(runVar.natco))
        .withColumn("year", lit(runVar.year)), outputPath + "activity_yearly/", writeMode = "overwrite", Seq("natco", "year"))
      writeWithFixedEmptyDFs(outputData.ProvisionedYearly
        .withColumn("natco", lit(runVar.natco))
        .withColumn("year", lit(runVar.year)), outputPath + "provisioned_yearly/", writeMode = "overwrite", Seq("natco", "year"))
      writeWithFixedEmptyDFs(outputData.RegisteredYearly
        .withColumn("natco", lit(runVar.natco))
        .withColumn("year", lit(runVar.year)), outputPath + "registered_yearly/", writeMode = "overwrite", Seq("natco", "year"))
    }
    // Always write user_agents
    ParquetWriter(outputData.UserAgents, persistencePath + "User_agents_tmp.parquet").writeParquetData(writeMode = "overwrite", partitionBy = false, null)
    val newUserAgents = new ParquetReader(persistencePath + "User_agents_tmp.parquet", persistencePath + "User_agents_tmp.parquet").read()
    ParquetWriter(newUserAgents, persistencePath + "User_agents.parquet").writeParquetData(writeMode = "overwrite", partitionBy = false, null)

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    hdfs.delete(new Path(persistencePath + "User_agents_tmp.parquet"), true)
  }
}