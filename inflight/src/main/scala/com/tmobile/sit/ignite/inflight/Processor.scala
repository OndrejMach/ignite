package com.tmobile.sit.ignite.inflight

import com.tmobile.sit.common.Logger

trait Processor extends Logger {
  def executeCalculation() : Unit
}
