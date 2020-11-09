package com.tmobile.sit.ignite.inflight.processing.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.common.readers.ExchangeRatesStageReader
import com.tmobile.sit.ignite.inflight.config.StageFiles
import com.tmobile.sit.ignite.inflight.datastructures.InputStructures
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{ExchangeRates, MapVoucher, OrderDB}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
 * A wrapper class for getting regerence (stage) data - dependencies are hotspot (orderDB, mapVoucher) and exchange rates
 * @param stageFiles - stageFiles configuration
 * @param sparkSession - yes it's needed
 */

class ReferenceData(stageFiles: StageFiles)(implicit sparkSession: SparkSession) extends Logger{


  val voucher = {
    import sparkSession.implicits._
    val path = stageFiles.path.get + stageFiles.voucherfile.get
    logger.info(s"Reading reference data ${path}")
    sparkSession.read.parquet(path)

      .withColumn("wlan_request_date", $"wlan_request_date".cast(StringType))
      .drop("year", "month", "day")
      .as[MapVoucher]
  }

  val orderDB = {
    import sparkSession.implicits._
    val path = stageFiles.path.get + stageFiles.orderDBFile.get
    logger.info(s"Reading reference data ${path}")
    sparkSession.read.parquet(path)

      .withColumnRenamed("paytid", "payid")
      .drop("year", "month", "day")
      .as[OrderDB]
  }

  val exchangeRates = {
    import sparkSession.implicits._
    val path=stageFiles.path.get + stageFiles.exchangeRatesFile.get
    logger.info(s"Reading reference data ${path}")

      ExchangeRatesStageReader(path).read()
      .drop("entry_id")
      .drop("load_date")
      .as[ExchangeRates]
  }
}
