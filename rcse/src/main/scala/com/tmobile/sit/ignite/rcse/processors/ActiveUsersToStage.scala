package com.tmobile.sit.ignite.rcse.processors

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inputs.ActiveUsersInputs
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * this class calculates list of active users.
 * @param sparkSession
 * @param settings - contains paths of the input files.
 */

class ActiveUsersToStage(implicit sparkSession: SparkSession, settings: Settings) extends Logger {

  def processData(): DataFrame = {
    val inputData = new ActiveUsersInputs()

    new activeusers.ActiveUsersProcessor(inputData, settings.app.processingDate).result


  }
}
