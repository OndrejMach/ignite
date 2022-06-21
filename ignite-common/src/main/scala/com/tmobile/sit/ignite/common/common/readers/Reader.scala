package com.tmobile.sit.ignite.common.common.readers

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Trait for any data reader.
 */

trait Reader extends Logger{
  def read(): DataFrame

  //def readFromPath(path: String): DataFrame
}
