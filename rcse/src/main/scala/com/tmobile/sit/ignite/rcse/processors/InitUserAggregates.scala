package com.tmobile.sit.ignite.rcse.processors

import java.sql.Date
import java.time.LocalDate

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.inituseragregates.InitUserAggregatesProcessor
import com.tmobile.sit.ignite.rcse.processors.inputs.{InitUserInputs, LookupsData}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class DatesCount(date_id: Date, rcse_reg_users_new: Int, rcse_reg_users_all: Int)


class InitUserAggregates(processingDate: Date, settings: Settings)(implicit sparkSession: SparkSession) extends Processor {

  val processingDateMinus1 = Date.valueOf(processingDate.toLocalDate.minusDays(1))
  val refDate = Date.valueOf(LocalDate.of(1900, 1, 1))

  override def processData(): Unit = {
    val inputData: InitUserInputs = new InitUserInputs(settings = settings)
    val lookups = new LookupsData(settings)

    val result = new InitUserAggregatesProcessor(inputData = inputData, lookups = lookups, maxData = MAX_DATE, processingDate = processingDate).getData

    result
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "false")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_x_rcse_init_user.TMD.csv");

    logger.info(s"result count ${result.count()}")

  }
}
