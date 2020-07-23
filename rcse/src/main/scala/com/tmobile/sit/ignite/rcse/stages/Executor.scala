package com.tmobile.sit.ignite.rcse.stages

import com.tmobile.sit.common.Logger

/**
 * Trait for all the processors
 */
trait Executor extends Logger {
  def runProcessing()
}
