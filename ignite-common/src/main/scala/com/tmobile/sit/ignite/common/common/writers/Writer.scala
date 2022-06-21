package com.tmobile.sit.ignite.common.common.writers

import com.tmobile.sit.ignite.common.common.Logger

/**
 * Writer interface for basically any writer class implemented here.
 */
trait Writer extends Logger {
  def writeData() : Unit
}
