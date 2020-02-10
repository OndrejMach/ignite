package com.tmobile.sit.ignite.common.writers

import com.tmobile.sit.ignite.common.Logger

trait Writer extends Logger {
  def writeData() : Unit
}
