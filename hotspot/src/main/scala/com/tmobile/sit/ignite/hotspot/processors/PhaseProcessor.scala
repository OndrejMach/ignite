package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.ignite.common.common.Logger

trait PhaseProcessor extends Logger{
  def process() : Unit
}
