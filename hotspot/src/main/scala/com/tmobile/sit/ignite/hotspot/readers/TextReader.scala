package com.tmobile.sit.ignite.hotspot.readers

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}


class TextReader(path: String)(implicit sparkSession: SparkSession) extends Reader{

  override def read(): DataFrame =
    sparkSession
      .read
      .text(path)
}
