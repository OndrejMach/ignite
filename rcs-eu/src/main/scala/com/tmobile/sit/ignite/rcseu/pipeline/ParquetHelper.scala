package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.ParquetReader
import com.tmobile.sit.ignite.rcseu.ParquetApplication.{fileMask, runVar}
import com.tmobile.sit.ignite.rcseu.config.{Settings, Setup}
import com.tmobile.sit.ignite.rcseu.data.FileSchemas
import org.apache.spark.sql.{DataFrame, SparkSession}




class ParquetHelper() (implicit sparkSession: SparkSession) extends Help {

  override def getArchiveFileMask():String = {
    // if yearly reprocessing or update on 31st of January
    if(runVar.runMode.equals("yearly") ||
      (runVar.runMode.equals("update") && runVar.date.endsWith("-12-31"))) {
      logger.info("Processing yearly archive data")
      runVar.year
    } else {
      logger.info("Processing daily and monthly archive data")
      runVar.month
    }
  }

  override def resolveActivity(sourceFilePath: String):DataFrame = {
    if(runVar.runMode.equals("update")) {
      logger.info("runMode: update")
      logger.info(s"Reading activity data for ${runVar.date} and ${runVar.tomorrowDate}")
      sparkSession.read
        .schema(FileSchemas.activitySchema)
        .option("mergeSchema", "True")
        .parquet(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.parquet*",
             sourceFilePath + s"activity_${runVar.tomorrowDate}*${runVar.natco}.parquet*")}
    else {
      logger.info(s"runMode: ${runVar.runMode}, reading daily activity")
      new ParquetReader(sourceFilePath + s"activity_${runVar.date}*${runVar.natco}.parquet*",
        schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read()
    }
  }


  override def resolvePath(settings:Settings):String = {
    // always reading daily data from the archive folder
    settings.archivePath.get
  }
}

