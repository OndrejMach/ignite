package com.tmobile.sit.ignite.inflight.processing

import org.apache.spark.sql.DataFrame

trait Processor {
  def executeProcessing() : DataFrame
}
