package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame

trait Processor extends Logger{
  def runProcessing(): DataFrame
}
