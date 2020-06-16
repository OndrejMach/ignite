package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.common.Logger

trait Processor extends Logger{
  def processData() : Unit
}
