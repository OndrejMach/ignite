package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import com.tmobile.sit.ignite.common.common.Logger
import org.apache.spark.sql.DataFrame

/**
 * Generic trait for processors
 */
trait Processor extends Logger{
  def runProcessing(): DataFrame
}
