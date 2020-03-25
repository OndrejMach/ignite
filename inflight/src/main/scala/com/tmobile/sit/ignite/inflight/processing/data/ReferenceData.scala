package com.tmobile.sit.ignite.inflight.processing.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.inflight.config.StageFiles
import com.tmobile.sit.ignite.inflight.datastructures.InputStructures
import com.tmobile.sit.ignite.inflight.datastructures.InputTypes.{ExchangeRates, MapVoucher, OrderDB}
import org.apache.spark.sql.SparkSession

class ReferenceData(stageFiles: StageFiles)(implicit sparkSession: SparkSession) extends Logger{


  val voucher = {
    import sparkSession.implicits._
    val path = stageFiles.path.get + stageFiles.voucherfile.get
    logger.info(s"Reading reference data ${path}")
    CSVReader(path,
      header = false,
      schema = Some(InputStructures.mapVoucherStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss" ,
      dateFormat = "yyyy-MM-dd")
      .read().as[MapVoucher]
  }

  val orderDB = {
    import sparkSession.implicits._
    val path = stageFiles.path.get + stageFiles.orderDBFile.get
    logger.info(s"Reading reference data ${path}")
    CSVReader(path,
      header = false,
      schema = Some(InputStructures.orderdbStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd")
      .read().as[OrderDB]
  }

  val exchangeRates = {
    import sparkSession.implicits._
    val path=stageFiles.path.get + stageFiles.exchangeRatesFile.get
    logger.info(s"Reading reference data ${path}")
    CSVReader(path,
      header = false,
      schema = Some(InputStructures.exchangeRatesStructure),
      delimiter = "|",
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd")
      .read().as[ExchangeRates]
  }
}
