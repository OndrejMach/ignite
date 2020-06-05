package com.tmobile.sit.ignite.inflight

/**
 * in case type of processing (monthly|daily) is not set as a commandline argument some simple help is printed here
 */

class HelperProcessor extends Processor {
  override def executeCalculation(): Unit = {
    logger.warn("Inflight ETL processing started - please specify the run mode!")
    logger.warn("Commandline parameters should be: 'daily' for daily calculateion, 'monthly' for monthly reports only'")
    logger.warn("Inflight processing skipped")
  }
}
