package com.tmobile.sit.ignite.hotspot.processors.staging

import java.sql.Date

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.Reader
import com.tmobile.sit.ignite.hotspot.data.CDRStructures
import com.tmobile.sit.ignite.hotspot.processors.udfs.DirtyStuff
import org.apache.spark.sql.functions.{dayofmonth, from_unixtime, lit, month, to_date, udf, when, year, trim, length}
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * preparing CDR input files for stage
 * @param cdrFileReader - CDRs data reader
 * @param fileDate - data date
 * @param encoderPath - path to 3DES encoder
 * @param sparkSession
 */

class CDRProcessor(cdrFileReader: Reader, fileDate: Date, encoderPath: String)(implicit sparkSession: SparkSession) extends Logger{

  def processData(): DataFrame = {
    import sparkSession.implicits._
    logger.info("Preparing CDR data.")
    val encoder3des = udf(DirtyStuff.encode)

    val data = cdrFileReader.read().as[String]

    val ret = data.filter($"value".startsWith(lit("D;"))).map(CDRStructures.CDRInput(_))
      .withColumn("ts", from_unixtime($"session_start_ts"-lit(com.tmobile.sit.ignite.hotspot.processors.fileprocessors.getTimeZoneOffset*3600)).cast(TimestampType))
      .withColumn("wlan_session_date", to_date($"ts").cast(DateType))
      .withColumn("user_name", lit("565C4BB4137DD2BFC1D2EA5EBC70ADB3"))
      .withColumn("user_name_extension", lit(null).cast(StringType))
      .withColumn("msisdn", when($"msisdn".isNotNull && (length($"msisdn") > 2),encoder3des(lit(encoderPath), $"msisdn") ).otherwise(lit(null).cast(StringType)))
      .na.fill(-1, Seq("wlan_user_account_id"))
      .withColumn("wlan_user_provider_code", lit(null).cast(StringType))
      .na.fill("undefined", Seq("hotspot_owner_id"))
      .withColumn("file_date", lit(fileDate).cast(DateType))
      .withColumn("year", year($"ts"))
      .withColumn("month", month($"ts"))
      .withColumn("day", dayofmonth($"ts"))
      .withColumn("framed_ip_address", when(trim($"framed_ip_address").equalTo(lit("")), lit(null).cast(StringType)).otherwise($"framed_ip_address"))
      .drop("ts")

    ret.printSchema()

    ret
  }
}
