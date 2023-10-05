package com.tmobile.sit.ignite.common.common.readers

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.functions.{col, concat_ws, format_string}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.StructType


/**
 * Reading Parquets and returning dataframe. For better testability all readers implement trait Reader.
 * There are two versions of Parquet readers - one reading a particular Parquet file and second capable of reading multiple Parquets
 * from a folder (Parquets must have the same schema of course).
 *
 * @author Ondrej Machacek
 */

/**
 * Abstract class to extract common characteristics of Parquet file rearing in spark. Any new Parquet reader alternative should inherit from this one.
 */
private[readers] abstract class ParquetGenericReader extends Logger {
  def getParquetReader(basePath: String, badRecordsPath: Option[String] = None)(implicit sparkSession: SparkSession): DataFrameReader = {
    val reader = sparkSession
      .read
      .option("basePath", basePath)
      .option("mergeSchema", "True")

    val invalidHandling = if (badRecordsPath.isDefined) {
      logger.info(s"Bad records to be stored in ${badRecordsPath.get}")
      reader.option("badRecordsPath", badRecordsPath.get)
    }
    else reader.option("mode", "DROPMALFORMED")

    invalidHandling

//    if (schema.isDefined) {
//      invalidHandling.schema(schema.get).option("basePath", basePath).option("mergeSchema", "True")
//    } else {
//      logger.warn("Schema file not defined, trying to infer one")
//      invalidHandling.option("basePath", basePath).option("inferschema", "true").option("mergeSchema", "True")
//    }
  }
}

/**
 * Basic Parquet reader reading a single file, returning dataframe.
 *
 * @param path            path to the file
 * @param badRecordsPath  path to the folder where invalid records will be stored - not used when None - default.
 * @param sparkSession    implicit SparkSession used for reading.
 */
class RCSEUParquetReader(path: String,
                         basePath: String,
                         badRecordsPath: Option[String] = None,
                         addFileDate: Boolean = false
                   ) (implicit sparkSession: SparkSession) extends ParquetGenericReader with Reader {

  def getParquetData(): DataFrame = {
    logger.info(s"Reading Parquet from path $path")
    val reader = getParquetReader(
      basePath,
      badRecordsPath
    )
    if (addFileDate){
      reader.parquet(path)
        .withColumn("month", format_string("%02d", col("month")))
        .withColumn("day", format_string("%02d", col("day")))
        .withColumn("FileDate", concat_ws("-", col("year"), col("month"), col("day")))
        .drop("natco", "year", "month", "day")
    }
    else{
      reader.parquet(path)
    }

  }

  override def read(): DataFrame = getParquetData()
}

/**
 * Companion object for the simple Parquet reader
 */
object RCSEUParquetReader {
  def apply(path: String,
            basePath: String,
            badRecordsPath: Option[String] = None
           )(implicit sparkSession: SparkSession): RCSEUParquetReader =

    new RCSEUParquetReader(path, basePath, badRecordsPath)(sparkSession)
}


/**
 * Basic Parquet reader reading multiple files, returning dataframe.
 *
 * @param paths            paths to the files
 * @param badRecordsPath  path to the folder where invalid records will be stored - not used when None - default.
 * @param schema          using spark types you can define Typed schema - it's highly recommended to us this parameter. By default schema is inferred.
 * @param sparkSession    implicit SparkSession used for reading.
 */
class RCSEUParquetMultiFileReader(paths: Seq[String],
                         basePath: String,
                         badRecordsPath: Option[String] = None,
                         schema: Option[StructType] = None,
                         addFileDate: Boolean = false
                        ) (implicit sparkSession: SparkSession) extends ParquetGenericReader with Reader {

  def getParquetData(): DataFrame = {
    logger.info(s"Reading Parquet from path $paths")
    val reader = getParquetReader(
      basePath,
      badRecordsPath
    )
    if (addFileDate){
      reader.parquet(paths: _*)
        .withColumn("month", format_string("%02d", col("month")))
        .withColumn("day", format_string("%02d", col("day")))
        .withColumn("FileDate", concat_ws("-", col("year"), col("month"), col("day")))
        .drop("natco", "year", "month", "day")
    }
    else{
      reader.parquet(paths: _*)
    }

  }

  override def read(): DataFrame = getParquetData()
}

/**
 * Companion object for the simple Parquet reader
 */
object RCSEUParquetMultiFileReader {
  def apply(paths: Seq[String],
            basePath: String,
            badRecordsPath: Option[String] = None,
            schema: Option[StructType] = None
           )(implicit sparkSession: SparkSession): RCSEUParquetMultiFileReader =

    new RCSEUParquetMultiFileReader(paths, basePath, badRecordsPath, schema)(sparkSession)
}

