package com.tmobile.sit.ignite.hotspot.processors


import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.common.implicits.TransformDataFrameColumns.TransformColumnNames
import com.tmobile.sit.ignite.hotspot.config.Settings
import com.tmobile.sit.ignite.hotspot.data.{ErrorCodes, OutputStructures, StagedataStructs}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * in this class all the output files are generated. Basically its only two steps - read stage, write output. Except for orderDBH where tiny logic is done hased on the transaction type
 * @param sparkSession
 * @param settings
 */

class OutputsProcessor(implicit sparkSession: SparkSession, settings: Settings) extends PhaseProcessor {
  private val UTF8CHAR = "\ufeff"

  private def writeOutput(data: DataFrame, filename: String, delimiter: String = "|", quote: String = "\"") = {
    logger.info(s"Writing output file: ${filename}")
    val firstColumn = data.columns(0)

    CSVWriter(
      data = data
        .withColumnRenamed(firstColumn,UTF8CHAR+firstColumn)
        .columnsToUpperCase()
        .repartition(1),
      writeHeader = true,
      delimiter = delimiter,
      timestampFormat = "yyyy-MM-dd HH:mm:ss",
      dateFormat = "yyyy-MM-dd",
      path = filename,
      quote = quote,
      quoteMode = "NONE",
      nullValue = null
    ).writeData()

  }

  private def adaptOrderDBH(data: DataFrame) = {
    import sparkSession.implicits._
    logger.info("Tweaking OrderDBH monetary values based on the transaction type")
    data
      .select(OutputStructures.ORDED_DB_H.head, OutputStructures.ORDED_DB_H.tail: _*)
      .withColumn("neg", when($"wlan_transac_type_id" === lit(1), lit(-1).cast(IntegerType)).otherwise(lit(1).cast(IntegerType)))
      .withColumn("amount_d_incl_vat", $"amount_d_incl_vat" * $"neg")
      .withColumn("amount_d_excl_vat", $"amount_d_excl_vat" * $"neg")
      .withColumn("amount_d_incl_vat_lc", $"amount_d_incl_vat_lc" * $"neg")
      .withColumn("amount_d_excl_vat_lc", $"amount_d_excl_vat_lc" * $"neg")
      .withColumn("amount_c_incl_vat", $"amount_c_incl_vat" * $"neg")
      .withColumn("amount_c_excl_vat", $"amount_c_excl_vat" * $"neg")
      .withColumn("amount_c_incl_vat_lc", $"amount_c_incl_vat_lc" * $"neg")
      .withColumn("amount_c_excl_vat_lc", $"amount_c_excl_vat_lc" * $"neg")
      .withColumn("amount_incl_vat", $"amount_incl_vat" * $"neg")
      .withColumn("amount_excl_vat", $"amount_excl_vat" * $"neg")
      .withColumn("amount_incl_vat_lc", $"amount_incl_vat_lc" * $"neg")
      .withColumn("amount_excl_vat_lc", $"amount_excl_vat_lc" * $"neg")
      .drop("neg")
  }

  override def process(): Unit = {
    val processingDate = settings.appConfig.processing_date.get.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val processingT = settings.appConfig.processing_date.get.toLocalDateTime
    logger.info(s"Starting processing for processing date ${processingDate} in the OUTPUT mode")
    logger.info("Writing output file for session_D")
    writeOutput(
      data = sparkSession
        .read
        .parquet(settings.stageConfig.session_d.get + s"/date=${processingDate}")
        .select(OutputStructures.SESSION_D.head, OutputStructures.SESSION_D.tail: _*),
      filename = settings.outputConfig.sessio_d.get
    )

    logger.info("Writing output file for orderDB_H")
    writeOutput(
      data = adaptOrderDBH(sparkSession.read.parquet(settings.stageConfig.orderDB_H.get + s"/date=${processingDate}")),
      filename = settings.outputConfig.orderDB_h.get
    )

    logger.info("Writing output file for Session_Q")
    writeOutput(
      data = sparkSession
        .read
        .parquet(settings.stageConfig.session_q.get + s"/date=${processingDate}")
        .select(OutputStructures.SESSION_Q.head, OutputStructures.SESSION_Q.tail: _*),
      filename = settings.outputConfig.session_q.get
    )

    logger.info("Writing output file for Error code")
    writeOutput(
      data = CSVReader(path = settings.stageConfig.error_codes_filename.get, header = true, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss",
        dateFormat = "yyyy-MM-dd", schema = Some(ErrorCodes.error_codes_struct))
        .read()
        .select(OutputStructures.ERROR_CODE.head, OutputStructures.ERROR_CODE.tail: _*),
      filename = settings.outputConfig.error_code.get
    )

    val hotspot = sparkSession.read.parquet(settings.stageConfig.wlan_hotspot_filename.get)

    //hotspot.filter("wlan_hotspot_id=1286234").show(false)
    logger.info("Writing output file for hotspot_ta_d")
    writeOutput(
      data = hotspot.sort("wlan_hotspot_ident_code","valid_to" ),
      filename = settings.outputConfig.hotspot_ta_d.get,
      delimiter = "~", quote = ""
    )

    val ht = hotspot
      .filter(col("valid_to") >= lit(com.tmobile.sit.ignite.hotspot.data.FUTURE).cast(TimestampType))
      .groupBy("wlan_hotspot_ident_code")
      .agg(max("valid_from").alias("valid_from")
      )
      .distinct()

    logger.info(s"COUNT HOTSPOT: ${ht.count()}")

    val columns = OutputStructures.HOTSPOT_VI_D.filter(_!= "wlan_hotspot_ident_code").map(i => last(i).alias(i))

    val dht = hotspot
      .filter(col("valid_to") >= lit(com.tmobile.sit.ignite.hotspot.data.FUTURE).cast(TimestampType))
      .join(ht, Seq("wlan_hotspot_ident_code", "valid_from"), "inner")
      .groupBy("wlan_hotspot_ident_code")
      .agg(columns.head ,columns.tail :_* )
      .distinct()


    logger.info("Writing output file for hotspot_vi_d")
    writeOutput(
      data = dht
        .select(OutputStructures.HOTSPOT_VI_D.head, OutputStructures.HOTSPOT_VI_D.tail: _*),
      filename = settings.outputConfig.hotspot_vi_d.get,
      delimiter = "~", quote = ""
    )

    logger.info("Writing output file for voucher")
    writeOutput(
      data = CSVReader(path = settings.stageConfig.wlan_voucher.get, header = true, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss",
        dateFormat = "yyyy-MM-dd")
        .read()
        .withColumn("duration_hours",
          when(col("duration").isNotNull,
            substring(col("duration"),1, 2).cast(IntegerType) * 24 +
              substring(col("duration"),4, 2).cast(IntegerType) +
              round(substring(col("duration"),7, 2).cast(DoubleType) / 60.0, 2)
          ).otherwise(lit(null).cast(DoubleType)))
        .select(OutputStructures.VOUCHER.head, OutputStructures.VOUCHER.tail: _*)
      ,
      filename = settings.outputConfig.voucher.get
    )

    logger.info("Writing output file for City")
    writeOutput(
      data = CSVReader(path = settings.stageConfig.city_data.get, header = true, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss",
        dateFormat = "yyyy-MM-dd")
        .read()
        .select(OutputStructures.CITY.head, OutputStructures.CITY.tail: _*)
      ,
      filename = settings.outputConfig.city.get
    )

    logger.info("Writing output file for country")
    writeOutput(
      data = CSVReader(path = settings.stageConfig.country.get, header = true, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss",
        dateFormat = "yyyy-MM-dd", schema = Some(StagedataStructs.country_code_structure))
        .read()
        .filter(col("valid_to") >= lit(com.tmobile.sit.ignite.hotspot.data.FUTURE).cast(TimestampType))
        .select(OutputStructures.COUNTRY.head, OutputStructures.COUNTRY.tail: _*)
      ,
      filename = settings.outputConfig.country.get
    )

    logger.info("Writing output file for Failed Transactions")
    writeOutput(
      data = sparkSession
        .read
        .parquet(settings.stageConfig.failed_transactions.get)
        .select(OutputStructures.FAILED_TRANSACTIONS.head, OutputStructures.FAILED_TRANSACTIONS.tail: _*)
      ,
      filename = settings.outputConfig.failed_trans.get
    )

    logger.info("Writing output file for Failed Logins")
    writeOutput(
      data = sparkSession
        .read
        .parquet(settings.stageConfig.failed_logins.get + s"/date=${processingDate}")
        .withColumnRenamed("login_datetime", "login_date")
        .select(OutputStructures.FAILED_LOGINS.head, OutputStructures.FAILED_LOGINS.tail: _*),
      filename = settings.outputConfig.failed_login.get
    )

    logger.info("Writing output file for Login Error")
    writeOutput(
      data = CSVReader(path = settings.stageConfig.login_errors.get, header = true, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss",
        dateFormat = "yyyy-MM-dd", schema = Some(ErrorCodes.loginErrorStruct)).read()
        .select(OutputStructures.LOGIN_ERROR.head, OutputStructures.LOGIN_ERROR.tail: _*),
      filename = settings.outputConfig.login_error.get
    )

  }
}
