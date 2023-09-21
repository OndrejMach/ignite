package com.tmobile.sit.ignite.common.common.readers

import com.tmobile.sit.ignite.common.common.Logger
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
  def getParquetReader(basePath: String, badRecordsPath: Option[String] = None,
                       schema: Option[StructType] = None)(implicit sparkSession: SparkSession): DataFrameReader = {
    val reader = sparkSession
      .read

    val invalidHandling = if (badRecordsPath.isDefined) {
      logger.info(s"Bad records to be stored in ${badRecordsPath.get}")
      reader.option("basePath", basePath).option("badRecordsPath", badRecordsPath.get)
    }
    else reader.option("basePath", basePath).option("mode", "DROPMALFORMED").option("mergeSchema", "True")

    if (schema.isDefined) {
      invalidHandling.schema(schema.get).option("basePath", basePath).option("mergeSchema", "True")
    } else {
      logger.warn("Schema file not defined, trying to infer one")
      invalidHandling.option("basePath", basePath).option("inferschema", "true").option("mergeSchema", "True")
    }
  }
}

/**
 * Basic Parquet reader reading a single file, returning dataframe.
 *
 * @param path            path to the file
 * @param badRecordsPath  path to the folder where invalid records will be stored - not used when None - default.
 * @param schema          using spark types you can define Typed schema - it's highly recommended to us this parameter. By default schema is inferred.
 * @param sparkSession    implicit SparkSession used for reading.
 */
class ParquetReader(path: String,
                    basePath: String,
                    badRecordsPath: Option[String] = None,
                    schema: Option[StructType] = None
                   ) (implicit sparkSession: SparkSession) extends ParquetGenericReader with Reader {

  private def getParquetData(path: String): DataFrame = {
    logger.info(s"Reading Parquet from path $path")
    val reader = getParquetReader(
      basePath,
      badRecordsPath
    )
    reader.parquet(path)

  }

  override def read(): DataFrame = getParquetData(path)
}

/**
 * A little enhanced Parquet reader capable of reading multiple Parquet files from a single path. Result is a single dataframe (union-ed data from each Parquet).
 * Parquet files must have the same structure.
 *
 * @param path            path to the file
 * @param fileList        list of filenames to be read from the path
 * @param badRecordsPath  path to the folder where invalid records will be stored - not used when None - default.
 * @param schema          using spark types you can define Typed schema - it's highly recommended to us this parameter. By default schema is inferred.
 * @param sparkSession    implicit SparkSession used for reading.
 */
class ParquetMultifileReader(path: String, basePath: String, fileList: Seq[String],
                             badRecordsPath: Option[String] = None,
                             schema: Option[StructType] = None
                            )
                            (implicit sparkSession: SparkSession) extends ParquetGenericReader with Reader {
  private def getParquetData(path: String): DataFrame = {
    logger.info(s"Reading Parquet from path $path")
    val reader = getParquetReader(basePath, badRecordsPath, schema
    )
    reader.parquet(fileList.map(path + "/" + _): _*)
  }

  override def read(): DataFrame = getParquetData(path)
}

/**
 * Companion object for the simple Parquet reader
 */

object ParquetReader {
  def apply(path: String,
            basePath: String,
            badRecordsPath: Option[String] = None,
            schema: Option[StructType] = None
           )(implicit sparkSession: SparkSession): ParquetReader =

    new ParquetReader(path, basePath, badRecordsPath, schema)(sparkSession)
}

/**
 * Companion object for the multifile Parquet reader
 */
object ParquetMultifileReader {
  def apply(path: String,
            basePath: String,
            fileList: Seq[String],
            badRecordsPath: Option[String] = None,
            schema: Option[StructType] = None
           )
           (implicit sparkSession: SparkSession): ParquetMultifileReader =

    new ParquetMultifileReader(
      path, basePath, fileList,
      badRecordsPath, schema
    )(sparkSession)
}
