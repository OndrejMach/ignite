package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.common.Logger

trait PhaseProcessor extends Logger{
  def process() : Unit
}
