package com.tmobile.sit.ignite.common.writers
import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CSVWriter(path: String, data: DataFrame,
                delimiter: String = ",", writeHeader: Boolean = true,
                quote: String = "\"", escape: String = "\\",
                quoteMode: String = "MINIMAL" )(implicit sparkSession: SparkSession) extends Writer {

  private def merge(srcPath: String, dstPath: String): Unit = {
    logger.info(s"Merging spark output ${srcPath} into a single file ${dstPath}")
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.fullyDelete(new File(dstPath))
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
  }


  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", if (writeHeader) "true" else "false")
      .option("sep", delimiter)
      .option("quote", quote )
      .option("escape", escape)
      .option("quoteMode",quoteMode )
      .csv(path+"_tmp")

    merge(path+"_tmp", path)
  }
}
