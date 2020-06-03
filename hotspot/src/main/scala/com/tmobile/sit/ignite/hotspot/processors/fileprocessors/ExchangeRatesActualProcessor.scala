package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import com.tmobile.sit.common.readers.Reader
import com.tmobile.sit.ignite.hotspot.data.{ExchangeRates, StageStructures}
import org.apache.spark.sql.functions.{col, lit, round, when}
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
 * this class calculates new exchange rates - basically it outdates old records and adds new ones from input file. This means history of exchanges rates is always there.
 * @param exchangeRatesReader - reader for new exchange rates file
 * @param prevExchangeRatesReader - actual exchange rates reader - this file is overwritten
 * @param maxDate - maxdate for FUTURE definition :)
 * @param sparkSession
 */


class ExchangeRatesActualProcessor(exchangeRatesReader: Reader, prevExchangeRatesReader: Reader, maxDate: Date)(implicit sparkSession: SparkSession) extends Processor {

  private def processExchangeRates(in: Dataset[ExchangeRates], periodFrom: Timestamp, periodTo: Timestamp, maxDate: Date): DataFrame = {
    import sparkSession.implicits._
    def getFirst(first: Column, second: Column, third: Column): Column = {
      when(first.isNull,
        when(second.isNull, when(third.isNotNull, third)
        ).otherwise(second)
      ).otherwise(first)
    }

    logger.info(s"Getting data for exchange rate TYPE")
    val rE = in
      .filter(i => i.ratetype.get == "EURX" || i.ratetype.get == "T012")
      .withColumn("exchange_rate_code",
        when(col("ratetype").equalTo("EURX"), "D")
          .otherwise(when(col("ratetype").equalTo("T012"), "Q")))
      .withColumnRenamed("exchange_rate", "exchange_rate_avg")
      .withColumnRenamed("multiplierfromcurrency", "faktv_1")
      .withColumnRenamed("multipliertocurrency", "faktn_1")
      .withColumnRenamed("valid_from", "valid_from_1")
      .drop("row_id")
      .drop("ratetype")
    // .withColumn("validE", lit(1))

    logger.info(s"Getting data for SELL exchgange rates")
    val rB = in
      .filter(i => i.ratetype.get == "B" || i.ratetype.get == "T12B")
      .withColumnRenamed("exchange_rate", "exchange_rate_sell")
      .withColumnRenamed("multiplierfromcurrency", "faktv_2")
      .withColumnRenamed("multipliertocurrency", "faktn_2")
      .withColumnRenamed("valid_from", "valid_from_2")
      .drop("row_id")
      .drop("ratetype")
    //.withColumn("validB", lit(1))

    logger.info(s"Getting data for BUY exchgange rates")
    val rG = in
      .filter(i => i.ratetype.get == "G" || i.ratetype.get == "T12G")
      .withColumnRenamed("exchange_rate", "exchange_rate_buy")
      .withColumnRenamed("multiplierfromcurrency", "faktv_3")
      .withColumnRenamed("multipliertocurrency", "faktn_3")
      .withColumnRenamed("valid_from", "valid_from_3")
      .drop("row_id")
      .drop("ratetype")

    logger.info(s"Joining all together")
    rE
      .join(rB, Seq("fromcurrency", "tocurrency"))
      .join(rG, Seq("fromcurrency", "tocurrency"))
      .withColumn("faktv", getFirst($"faktv_1", $"faktv_2", $"faktv_3"))
      .withColumn("faktn", getFirst($"faktn_1", $"faktn_2", $"faktn_3"))
      .withColumn("valid_from", getFirst($"valid_from_1", $"valid_from_2", $"valid_from_3"))
      .withColumnRenamed("fromcurrency", "currency_code")
      .withColumn("period_from", lit(periodFrom).cast(TimestampType))
      .withColumn("period_to", lit(periodTo).cast(TimestampType))
      .withColumn("valid_to", lit(maxDate).cast(DateType))
      .withColumn("exchange_rate_avg", round($"exchange_rate_avg", 6))
      .withColumn("exchange_rate_buy", round($"exchange_rate_buy", 6))
      .withColumn("exchange_rate_sell", round($"exchange_rate_sell", 6))
      .withColumn("entry_id", lit(0))
      .withColumn("load_date", lit(Timestamp.valueOf(LocalDateTime.now())))
      .select(StageStructures.EXCHANGE_RATES_OUTPUT_COLUMNS.head, StageStructures.EXCHANGE_RATES_OUTPUT_COLUMNS.tail: _*)
  }

  def historise(newExchangeRates: DataFrame, oldDataExchangeRates: DataFrame) : DataFrame = {
   val joined =  oldDataExchangeRates
      .union(newExchangeRates)
      .distinct()
      .join(
        newExchangeRates.select(col("currency_code"), col("exchange_rate_code"),col("valid_to"), col("valid_from").alias("new_valid_from")),
        Seq("currency_code", "exchange_rate_code", "valid_to"),
        "left_outer"
      )
      logger.info(s"Historising count ${joined.filter(col("new_valid_from").isNotNull).count()}")

      joined.withColumn("valid_to", when(col("valid_from") < col("new_valid_from"),col("new_valid_from")).otherwise(col("valid_to")))
      //.drop("old_valid_from")
      .select(StageStructures.EXCHANGE_RATES_OUTPUT_COLUMNS.head, StageStructures.EXCHANGE_RATES_OUTPUT_COLUMNS.tail: _*)
  }

  override def runProcessing(): DataFrame = {
    logger.info("Reading exchange rates raw file")
    val exchRatesRaw = exchangeRatesReader.read()
    logger.info("Initialising exchange rates parser")
    val parser = new ExchangeRatesParser(exchRatesRaw)
    logger.info("Geting period dates")
    val periods = parser.getPeriodDates

    logger.info(s"Reading data")
    val exchRates = parser.getData
    logger.info("processing exchange rates")
    val newExchangeRates = processExchangeRates(in = exchRates, periodFrom = periods._1, periodTo = periods._2, maxDate)
    logger.info(s"got new exchange rates (count: ${newExchangeRates.count()})")
    logger.info("Reading old exchangeRates")
    val oldExchangeRates = prevExchangeRatesReader.read()
    logger.info("Historising data")
    historise(newExchangeRates, oldExchangeRates)
  }

}
