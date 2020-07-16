package com.tmobile.sit.ignite.rcse.stages

class Helper extends Executor {
  override def runProcessing(): Unit = {
    logger.error("Processing regime not specified, stopping")
  }
}
