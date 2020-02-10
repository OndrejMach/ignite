package com.tmobile.sit.ignite.common.writers

import com.tmobile.sit.ignite.common.Logger
import org.apache.spark.sql.DataFrame

trait Writer extends Logger {
  def writeData(data: DataFrame) : Unit
}
