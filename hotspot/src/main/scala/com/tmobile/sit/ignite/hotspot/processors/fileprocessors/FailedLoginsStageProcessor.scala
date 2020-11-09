package com.tmobile.sit.ignite.hotspot.processors.fileprocessors

import com.tmobile.sit.ignite.hotspot.config.Settings
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, lit, trim, when}
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class FailedLoginsStageProcessor(rawData: DataFrame, loginErrorCodes: DataFrame)(implicit settings: Settings, sparkSession: SparkSession) extends Processor {

  private lazy val errorCodesLookup = {
    logger.info("preparing error codes lookup")
    loginErrorCodes.select("error_desc", "error_id")
  }

  override lazy val runProcessing: DataFrame = {
    import sparkSession.implicits._
    logger.info("preprocessing input data")
    def fixEmptyString(columnName: String) = when(trim(col(columnName)).equalTo("") || col(columnName).isNull, lit("UNDEFINED")).otherwise(trim(col(columnName)))

    val ret = rawData
      .withColumn("login_attempt_ts", $"login_attempt_ts" - lit(2*3600))
      .withColumn("login_datetime", when($"hotspot_provider_code".equalTo(lit("TMUK")), from_unixtime($"login_attempt_ts" - lit(3600))).otherwise(from_unixtime($"login_attempt_ts")))
      .withColumn("login_date", $"login_datetime".cast(DateType))
      .filter($"login_date" === lit(settings.appConfig.processing_date.get).cast(TimestampType).cast(DateType))
      .withColumn("login_hour", date_format($"login_datetime", "yyyyMMddHH"))
      .na.fill("UNDEFINED", Seq("hotspot_country_code", "user_provider", "hotspot_ident_code", "hotspot_provider_code", "hotspot_venue_code", "hotspot_venue_type_code", "hotspot_city_name"))
      .withColumn("hotspot_country_code",fixEmptyString("hotspot_country_code"))
      .withColumn("user_provider",fixEmptyString("user_provider"))
      .withColumn("hotspot_ident_code",fixEmptyString("hotspot_ident_code"))
      .withColumn("hotspot_provider_code",fixEmptyString("hotspot_provider_code"))
      .withColumn("hotspot_venue_code",fixEmptyString("hotspot_venue_code"))
      .withColumn("hotspot_venue_type_code",fixEmptyString("hotspot_venue_type_code"))
      .withColumn("hotspot_city_name",fixEmptyString("hotspot_city_name"))
      .withColumn("hotspot_city_name", when($"hotspot_city_name".equalTo(lit("*")), "UNDEFINED").otherwise($"hotspot_city_name"))
      .withColumnRenamed("login_id", "tid")
      .join(errorCodesLookup, $"login_error_code" === $"error_desc", "left_outer")
      .drop("error_desc")
      .drop("login_error_code")
      .withColumnRenamed("error_id", "login_error_code")

    logger.info(s"Preprocessed data count: ${ret.count()}")
    ret
  }
}
