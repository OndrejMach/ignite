package com.tmobile.sit.ignite.rcse

import java.time.LocalDate

import com.tmobile.sit.common.readers.CSVReader
import java.sql.Date

import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object Application extends App {
  implicit val sparkSession = getSparkSession()
  val terminalPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_terminal.csv"
  val tacPath = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_tac.csv"
  val MAX_DATE = Date.valueOf(LocalDate.of(4712, 12, 31))
  val outputPath = "/Users/ondrejmachacek/tmp/rcse/cptm_ta_d_rcse_terminal.csv"

  val outputCols = Seq("rcse_terminal_id", "tac_code", "terminal_id",
    "rcse_terminal_vendor_sdesc", "rcse_terminal_vendor_ldesc",
    "rcse_terminal_model_sdesc", "rcse_terminal_model_ldesc",
    "modification_date")

  val terminal_d_struct = StructType(
    Seq(
      StructField("rcse_terminal_id", IntegerType, true),
      StructField("tac_code", StringType, true),
      StructField("terminal_id", IntegerType, true),
      StructField("rcse_terminal_vendor_sdesc", StringType, true),
      StructField("rcse_terminal_vendor_ldesc", StringType, true),
      StructField("rcse_terminal_model_sdesc", StringType, true),
      StructField("rcse_terminal_model_ldesc", StringType, true),
      StructField("modification_date", TimestampType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)
    )
  )
  val tac_struct = StructType(
    Seq(
      StructField("terminal_id", IntegerType, true),
      StructField("tac_code", StringType, true),
      StructField("id", IntegerType, true),
      StructField("manufacturer", StringType, true),
      StructField("model", StringType, true),
      StructField("model_alias", StringType, true),
      StructField("csso_alias", StringType, true),
      StructField("status", StringType, true),
      StructField("international_material_number", StringType, true),
      StructField("launch_date", DateType, true),
      StructField("gsm_bandwidth", StringType, true),
      StructField("gprs_capable", StringType, true),
      StructField("edge_capable", StringType, true),
      StructField("umts_capable", StringType, true),
      StructField("wlan_capable", StringType, true),
      StructField("form_factor", StringType, true),
      StructField("handset_tier", StringType, true),
      StructField("wap_type", StringType, true),
      StructField("wap_push_capable", StringType, true),
      StructField("colour_depth", StringType, true),
      StructField("mms_capable", StringType, true),
      StructField("camera_type", StringType, true),
      StructField("camera_resolution", StringType, true),
      StructField("video_messaging_capable", StringType, true),
      StructField("ringtone_type", StringType, true),
      StructField("java_capable", StringType, true),
      StructField("email_client", StringType, true),
      StructField("email_push_capable", StringType, true),
      StructField("operating_system", StringType, true),
      StructField("golden_gate_user_interface", StringType, true),
      StructField("tzones_hard_key", StringType, true),
      StructField("bluetooth_capable", StringType, true),
      StructField("tm3_capable", StringType, true),
      StructField("terminal_full_name", StringType, true),
      StructField("video_record", StringType, true),
      StructField("valid_from", DateType, true),
      StructField("valid_to", DateType, true),
      StructField("entry_id", IntegerType, true),
      StructField("load_date", TimestampType, true)

    )
  )


  val terminalDData = CSVReader(path = terminalPath,
    header = false,
    schema = Some(terminal_d_struct),
    delimiter = "|")
    .read()

  val maxTerminalID = terminalDData
    .select(max("rcse_terminal_id"))
    .collect()(0)
    .getInt(0)

  println(maxTerminalID)

  import sparkSession.implicits._

  val terminalFiltered =
    terminalDData.filter($"tac_code".isNotNull)

  val terminalNullTACCode =
    terminalDData.filter($"tac_code".isNull)

  terminalFiltered.show(false)

  println(s" ${terminalFiltered.count()}, ${terminalNullTACCode.count()}")

  val tac = CSVReader(
    path = tacPath,
    header = false,
    schema = Some(tac_struct),
    delimiter = "|"
  ).read()

  tac.show(false)

  val tacFiltered = tac.filter($"valid_to" >= lit(MAX_DATE) && $"status".isNotNull &&
    ($"status" === lit("ACTIVE") ||
      ($"status".substr(0, 2) === lit("NO") &&
        ($"status".substr(length($"status") - lit(4), lit(4)) === lit("INFO"))
        )
      )
  )
    .select($"terminal_id", $"tac_code", $"id", $"manufacturer", $"model")

  val tacProcessed = tacFiltered
    .select($"terminal_id", $"tac_code".substr(0, 6).as("tac_code"), $"id", $"manufacturer", $"model")
    .distinct()
    .groupBy("tac_code")
    .agg(
      count("*").alias("count"),
      first("terminal_id").alias("terminal_id"),
      first("id").alias("id"),
      first("manufacturer").alias("manufacturer"),
      first("model").alias("model")
    )
    .filter($"count" === lit(1))
    .drop("count")
    .union(tacFiltered)
    .drop("terminal_id")


  val joinTacTerminal = terminalFiltered
    .join(tacProcessed, Seq("tac_code"), "left")

  val tacNotNull = joinTacTerminal
    .filter($"id".isNotNull)
    .withColumn("tac_code", lit(null).cast(StringType))
    .withColumn("terminal_id", $"id")
    .withColumn("modification_date", $"load_date")
    .select("rcse_terminal_id", "tac_code",
      "rcse_terminal_vendor_sdesc", "rcse_terminal_vendor_ldesc",
      "rcse_terminal_model_sdesc", "rcse_terminal_model_ldesc",
      "modification_date", "terminal_id")


  val tacNull = joinTacTerminal
    .filter($"id".isNull)
    .select("rcse_terminal_id", "tac_code",
      "rcse_terminal_vendor_sdesc", "rcse_terminal_vendor_ldesc",
      "rcse_terminal_model_sdesc", "rcse_terminal_model_ldesc",
      "modification_date")
    .withColumn("terminal_id", lit(null).cast(IntegerType))


  val cols = terminalDData.columns.map(_ + "_orig")
  val join2I = tacNotNull
    .join(terminalDData.toDF(cols: _*), $"rcse_terminal_id" === $"rcse_terminal_id_orig", "left")
    .withColumn("rcse_terminal_id", when(
      $"rcse_terminal_vendor_sdesc_orig".isNotNull && $"rcse_terminal_vendor_ldesc_orig".isNotNull &&
        $"rcse_terminal_model_sdesc_orig".isNotNull && $"rcse_terminal_model_ldesc_orig".isNotNull &&
        $"rcse_terminal_model_ldesc" === $"rcse_terminal_model_ldesc_orig" &&
        $"rcse_terminal_model_sdesc" === $"rcse_terminal_model_sdesc_orig",
      $"rcse_terminal_id").otherwise(when(
      $"rcse_terminal_id_orig".isNotNull, $"rcse_terminal_id_orig").otherwise(lit(-1))
    )
    )
    .select(tacNotNull.columns.head, tacNotNull.columns.tail :_*)

  def tuneDF(data: DataFrame): DataFrame = {
    data
    .sort("rcse_terminal_id","tac_code","terminal_id","rcse_terminal_vendor_sdesc","rcse_terminal_model_sdesc","modification_date")
      .groupBy("rcse_terminal_id","tac_code","terminal_id","rcse_terminal_vendor_sdesc","rcse_terminal_model_sdesc")
      .agg(
        first("modification_date").alias("modification_date"),
        first("rcse_terminal_vendor_ldesc").alias("rcse_terminal_vendor_ldesc"),
        first("rcse_terminal_model_ldesc").alias("rcse_terminal_model_ldesc")
      )
  }

  val join2 = tuneDF(join2I.filter($"rcse_terminal_id" =!= lit(-1)))

  val nullTerminalId =
    tuneDF(join2I.filter($"rcse_terminal_id" === lit(-1)))
    .withColumn("rcse_terminal_id", monotonically_increasing_id() + lit(maxTerminalID))

  join2.printSchema()
  nullTerminalId.printSchema()
  tacNull.printSchema()
  terminalNullTACCode.printSchema()


  val result = join2.select(outputCols.head, outputCols.tail :_*)
    .union(nullTerminalId.select(outputCols.head, outputCols.tail :_*))
    .union(tacNull.select(outputCols.head, outputCols.tail :_*))
    .union(terminalNullTACCode.select(outputCols.head, outputCols.tail :_*))

  println(result.count())
//TODO quotation
  CSVWriter(
    data = result,
    path = outputPath,
    delimiter = "|"
  ).writeData()

}
