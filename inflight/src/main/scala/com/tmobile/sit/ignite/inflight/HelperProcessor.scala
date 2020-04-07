package com.tmobile.sit.ignite.inflight

class HelperProcessor extends Processor {
  override def executeCalculation(): Unit = {
    logger.warn("Inflight ETL processing started - please specify the run mode!")
    logger.warn("Commandline parameters should be: 'daily' for daily calculateion, 'monthly' for monthly reports only'")
    logger.warn("Inflight processing skipped")
  }
}
