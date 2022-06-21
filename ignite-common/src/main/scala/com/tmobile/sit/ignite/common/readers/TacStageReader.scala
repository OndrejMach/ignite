package com.tmobile.sit.ignite.common.readers

import com.tmobile.sit.ignite.common.common.readers.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

class TacStageReader(path: String)(implicit sparkSession: SparkSession) extends Reader {
  override def read(): DataFrame = {
    val data = sparkSession
      .read
      .parquet(path)

    data
  }
}

object TacStageReader {
  def apply(path: String)(implicit sparkSession: SparkSession): TacStageReader = new TacStageReader(path)(sparkSession)
}
