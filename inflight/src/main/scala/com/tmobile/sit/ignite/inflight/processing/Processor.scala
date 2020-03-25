package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame

trait Processor extends Logger{
  def executeProcessing() : DataFrame
}
