package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inituseragregates.InitUserAggregatesProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitUserInputs, LookupsDataReader}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class DatesCount(date_id: Date, rcse_reg_users_new: Int, rcse_reg_users_all: Int)

/**
 * processor for init user aggregates. Reads inputs and calculates data
 * @param sparkSession
 * @param settings - paths from where to read the input data
 */

class InitUserAggregates(implicit sparkSession: SparkSession,settings: Settings) extends Logger {

   def processData(): DataFrame = {
    val inputData: InitUserInputs = new InitUserInputs()
    val lookups = new LookupsDataReader()

    new InitUserAggregatesProcessor(inputData = inputData, lookups = lookups, maxDate = settings.app.maxDate, processingDate = settings.app.processingDate).getData

  }
}
