package com.tmobile.sit.ignite.hotspot.processors

/**
 * just a helper processor if processing regime is not properly specifies.
 */
class HelperProcessor extends PhaseProcessor {

  override def process(): Unit = {
    logger.error("Processing mode unspecified")
  }
}
