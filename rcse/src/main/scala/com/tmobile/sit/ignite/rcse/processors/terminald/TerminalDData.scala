package com.tmobile.sit.ignite.rcse.processors.terminald

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Wrapper class containing preprocessed data for the Terminal_d processing
 * @param terminalDData - actual termianl_d data
 * @param sparkSession
 */

class TerminalDData(terminalDData: DataFrame)(implicit sparkSession: SparkSession) extends Logger {
  import sparkSession.implicits._

  val maxTerminalID = {
    logger.info("Getting max ID")
    terminalDData
      .select(max("rcse_terminal_id").cast(IntegerType))
      .collect()(0)
      .getInt(0)
  }


  val terminalFiltered = {
    logger.info("Checking data for valid tac_code")
    terminalDData.filter($"tac_code".isNotNull)
  }

  val terminalNullTACCode = {
    logger.info("Checking data for tac_code null")
    terminalDData.filter($"tac_code".isNull)
  }
}
