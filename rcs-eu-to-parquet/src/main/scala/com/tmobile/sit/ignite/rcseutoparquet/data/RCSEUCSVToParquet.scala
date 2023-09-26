package com.tmobile.sit.ignite.rcseutoparquet.data



import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.common.common.readers.{CSVReader, ParquetReader}
import com.tmobile.sit.ignite.common.common.writers.RCSEUParquetWriter
import com.tmobile.sit.ignite.rcseutoparquet.Application.runVar
import com.tmobile.sit.ignite.rcseu.config.{Settings, Setup}
import com.tmobile.sit.ignite.rcseu.data.FileSchemas
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transform extends Logger{
  def csvToParquet(inputFilePath: String, sourceFilePath: String)(implicit sparkSession: SparkSession): Unit
}

object RCSEUCSVToParquet extends Transform {

  def addPartitionCols(data: DataFrame): DataFrame = {
    data
      .withColumn("natco", lit(runVar.natco))
      .withColumn("year", lit(runVar.year))
      .withColumn("month", lit(runVar.monthNum))
      .withColumn("day", lit(runVar.dayNum))
  }
  def csvToParquet(inputFilePath: String, sourceFilePath: String)(implicit sparkSession: SparkSession): Unit = {
    println(runVar.date)
    logger.info(s"Reading activity csv data for ${runVar.date}")
    val activityData = new CSVReader(inputFilePath + s"activity_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.activitySchema), header = true, delimiter = "\t").read()

    logger.info(s"Writing activity parquet data for ${runVar.date}")
    RCSEUParquetWriter(addPartitionCols(activityData), sourceFilePath + s"activity/").writeParquetData(writeMode = "overWrite", partitionCols = Seq("natco", "year", "month", "day"))

    logger.info(s"Reading provision csv data for ${runVar.date}")
    val provisionData = new CSVReader(inputFilePath + s"provision_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.provisionSchema), header = true, delimiter = "\t").read()

    logger.info(s"Writing provision parquet data for ${runVar.date}")
    RCSEUParquetWriter(addPartitionCols(provisionData), sourceFilePath + s"provision/").writeParquetData(writeMode = "overWrite", partitionCols = Seq("natco", "year", "month", "day"))

    logger.info(s"Reading register requests csv data for ${runVar.date}")
    val registerRequestsData = new CSVReader(inputFilePath + s"register_requests_${runVar.date}*${runVar.natco}.csv*",
      schema = Some(FileSchemas.registerRequestsSchema), header = true, delimiter = "\t").read()

    logger.info(s"Writing register requests parquet data for ${runVar.date}")
    RCSEUParquetWriter(addPartitionCols(registerRequestsData), sourceFilePath + s"register_requests/").writeParquetData(writeMode = "overWrite",
      partitionCols = Seq("natco", "year", "month", "day"))
  }
}