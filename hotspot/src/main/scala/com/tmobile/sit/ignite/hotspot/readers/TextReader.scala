package com.tmobile.sit.ignite.hotspot.readers

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * helper reader for Generic text
 * @param path - what to read
 * @param sparkSession
 */

class TextReader(path: String)(implicit sparkSession: SparkSession) extends Reader{

  override def read(): DataFrame =
    sparkSession
      .read
      .text(path)
}
