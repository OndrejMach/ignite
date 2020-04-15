package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.common.Logger

/**
 * how processors should looks like.
 */
trait Processor extends Logger {
  def executeCalculation() : Unit
}
