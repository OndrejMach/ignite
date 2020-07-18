package com.tmobile.sit.ignite.rcse.stages

/**
 * handling the situation when processing regime is not set properly (as a commandline argument)
 */
class Helper extends Executor {
  override def runProcessing(): Unit = {
    logger.error("Processing regime not specified, stopping")
  }
}
