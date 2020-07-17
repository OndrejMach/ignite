package com.tmobile.sit.ignite.rcse.processors.inputs

import java.sql.Date

import com.tmobile.sit.common.Logger

abstract class InputData(processingDate: Date) extends Logger {
  protected val todaysPartition = s"/date=${processingDate}"
  protected val yesterdaysPartition = s"/date=${Date.valueOf(processingDate.toLocalDate.minusDays(1))}"
}
