package com.tmobile.sit.ignite.rcse.writer

import java.sql.Date

import com.tmobile.sit.ignite.rcse.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * case class containing data for all the aggregate files
 * @param initConf - init conf data
 * @param initUser - init user data
 * @param uauAggregates - uau aggregates data
 */
case class AggregatesData(initConf: DataFrame, initUser: DataFrame, uauAggregates: DataFrame)

/**
 * this writer writes all files for the aggregates outputs
 * @param processingDate - used for partitioning
 * @param data - data for all the aggregate outputs
 * @param sparkSession
 * @param settings - contains paths where to store files - in the stage area
 */

class AggregatesWriter(processingDate: Date, data: AggregatesData)(implicit sparkSession: SparkSession, settings: Settings ) extends RCSEWriter(processingDate = processingDate) {
  def writeData() = {
    logger.info("Writing init user aggregates")
    writeParquet(data.initUser, settings.stage.initUser, true)

    logger.info("Writing init conf aggregates")
    writeParquet(data.initConf, settings.stage.initConf, true)

    logger.info("Writing UAU aggregates")
    writeParquet(data.uauAggregates, settings.stage.uauFile, true)

  }

}
