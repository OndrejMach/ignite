package com.tmobile.sit.ignite.rcse.processors.inputs

import java.sql.Date

import com.tmobile.sit.ignite.common.common.Logger

/**
 * Abstract class containing vals with partition paths. partitioning for all the stage files is the same.
 * @param processingDate - the key for the aggregation
 */

abstract class InputData(processingDate: Date) extends Logger {
  protected val todaysPartition = s"/date=${processingDate}/"
  protected val yesterdaysPartition = s"/date=${Date.valueOf(processingDate.toLocalDate.minusDays(1))}/"
}
