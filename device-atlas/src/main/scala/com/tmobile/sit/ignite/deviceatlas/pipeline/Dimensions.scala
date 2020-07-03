package com.tmobile.sit.ignite.deviceatlas.pipeline

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


trait DimensionProcessing extends Logger{
  def update_d_tac(terminalDB: DataFrame, d_tac: DataFrame, stagePath: String, ODATE: String): DataFrame
  def update_d_terminal(terminalDB: DataFrame, d_terminal: DataFrame, stagePath: String, ODATE: String): DataFrame
}


class Dimensions(implicit sparkSession: SparkSession) extends DimensionProcessing {

  import sparkSession.sqlContext.implicits._

  final val max_date = "4712-12-31"
  val today = Calendar.getInstance().getTime()
  val formatStr = new SimpleDateFormat("yyyyMMdd")
  val formatYMD = new SimpleDateFormat("yyyy-MM-dd")
  val formatYMDHms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val formatInt = new SimpleDateFormat("yyyyMMddHHmm")

  def historize_dim(input: DataFrame, dimension: DataFrame, schema: Array[String], lookup_key: Seq[String], ODATE: String): DataFrame = {
    // list of columns to use for comparing records. Removing technical columns
    val compare_columns = dimension.first().schema.fieldNames.toSeq
      .filter(_ != "valid_from")
      .filter(_ != "valid_to")
      .filter(_ != "entry_id")
      .filter(_ != "load_date")

    // get closed
    val closed_records = dimension.where(s"valid_to != '$max_date'")

    // get open
    val open_records = dimension.where(s"valid_to = '$max_date'")

    // join to get unchanged
    val unchanged_records = open_records.join(input, lookup_key, "leftanti")
      .select(schema.head, schema.tail:_*)

    // join to get updates - records present in both tables
    val possible_updates = input.join(open_records.select(lookup_key.head, lookup_key.tail:_*), lookup_key, "inner")
        .drop(lookup_key.map(x=>"open_records.".concat(x)):_*)

    // test all 'compare_columns' to identify unchanged records
    val no_update = open_records.na.fill("")
      .join(possible_updates.drop("valid_from", "valid_to", "load_date", "entry_id").na.fill(""), compare_columns, "inner")
      .select(open_records.columns.head, open_records.columns.tail:_*)

    // remove unchanged (no_update) records from possible updates. Possible updates = input file values ==> inserts to DB
    val update_inserts = possible_updates.join(no_update, lookup_key, "leftanti").select(possible_updates.columns.head, possible_updates.columns.tail:_*)
      .select(schema.head, schema.tail:_*)
    // select existing DB records and update valid_to column
    val update_updates = open_records
      .join(update_inserts.select(lookup_key.head, lookup_key.tail:_*), lookup_key, "inner")
      .drop(lookup_key.map(x=>"update_inserts.".concat(x)):_*)
      .withColumn("valid_to", lit(formatYMD.format(formatStr.parse(ODATE))))
      .select(schema.head, schema.tail:_*)

    // join to get new
    val new_records = input.join(open_records, lookup_key, "leftanti")
      .select(schema.head, schema.tail:_*)
      .join(closed_records, lookup_key, "leftanti") // to ignore closed records. Once closed, never updated again?
      .select(schema.head, schema.tail:_*)

    logger.info("done")

    new_records
      .union(update_inserts)
      .union(update_updates)
      .union(no_update)
      .union(unchanged_records)
      .union(closed_records)
    .sort(lookup_key.head, lookup_key.tail:+"valid_to":_*)

  }

  override def update_d_tac(terminalDB: DataFrame, d_tac: DataFrame, stagePath: String, ODATE: String): DataFrame = {
    logger.info("Updating cptm_ta_d_tac data")

    val lookup_key = Seq("tac_code")
    val schema : Array[String] = d_tac.columns
    // manual select is here mainly because of the tmo_tac_irr_$ODATE.csv output file, plus trimming of some columns
    val tac_data = terminalDB
      .select(
        "tac_code",
        "terminal_id",
        "id",
        "manufacturer",
        "model",
        "model_alias",
        "terminal_full_name",
        "csso_alias",
        "status",
        "international_material_number",
        "launch_date",
        "gsm_bandwidth",
        "gprs_capable",
        "edge_capable",
        "umts_capable",
        "wlan_capable",
        "form_factor",
        "handset_tier",
        "wap_type",
        "wap_push_capable",
        "colour_depth",
        "mms_capable",
        "camera_type",
        "camera_resolution",
        "video_messaging_capable",
        "video_record",
        "ringtone_type",
        "java_capable",
        "email_client",
        "email_push_capable",
        "operating_system",
        "golden_gate_user_interface",
        "tzones_hard_key",
        "bluetooth_capable",
        "tm3_capable")
      .withColumn("manufacturer", substring($"manufacturer",0 , 50))
      .withColumn("model", substring($"model",0 , 50))
      .withColumn("model_alias", substring($"model_alias",0 , 255))
      .withColumn("terminal_full_name", substring($"terminal_full_name",0 , 200))
.cache()

    CSVWriter(data = tac_data, path = s"${stagePath}tmo_tac_irr_$ODATE.csv", delimiter = "|", writeHeader = false, escape = "", quote = "").writeData()
    val cols = d_tac.columns.toSeq
      .filter(_ != "valid_from")
      .filter(_ != "valid_to")
      .filter(_ != "entry_id")
      .filter(_ != "load_date")

    val tac_data_sorted = tac_data.select(cols.head, cols.tail:_*)
      .withColumn("launch_date",
        when($"launch_date" === "1900-01-01", "")
          .otherwise($"launch_date"))
      .withColumn("valid_from", lit(formatYMD.format(formatStr.parse(ODATE))))
      .withColumn("valid_to", lit(max_date))
      .withColumn("entry_id", lit(formatInt.format(today)))
      .withColumn("load_date", lit(formatYMDHms.format(today)))
      .sort("tac_code")

    historize_dim(tac_data_sorted, d_tac, schema, lookup_key, ODATE)

  }

  override def update_d_terminal(terminalDB: DataFrame, d_terminal: DataFrame, stagePath: String, ODATE: String): DataFrame = {
    logger.info("Updating cptm_ta_d_terminal data")
    val exploded  =  terminalDB.sort( $"terminal_id", $"tac_code".desc).dropDuplicates("terminal_id")
      .withColumn("specs", map(
        lit("MANUFACTURER"),                  $"manufacturer",
        lit("MODEL"),                         $"model",
        lit("MODEL_ALIAS"),                   $"model_alias",
        lit("TERMINAL_FULL_NAME"),            $"terminal_full_name",
        lit("CSSO_ALIAS"),                    $"csso_alias",
        lit("STATUS"),                        $"status",
        lit("INTERNATIONAL_MATERIAL_NUMBER"), $"international_material_number",
        lit("LAUNCH_DATE"),                   $"launch_date",
        lit("GSM_BANDWIDTH"),                 $"gsm_bandwidth",
        lit("GPRS_CAPABLE"),                  $"gprs_capable",
        lit("EDGE_CAPABLE"),                  $"edge_capable",
        lit("UMTS_CAPABLE"),                  $"umts_capable",
        lit("WLAN_CAPABLE"),                  $"wlan_capable",
        lit("FORM_FACTOR"),                   $"form_factor",
        lit("HANDSET_TIER"),                  $"handset_tier",
        lit("WAP_TYPE"),                      $"wap_type",
        lit("WAP_PUSH_CAPABLE"),              $"wap_push_capable",
        lit("COLOUR_DEPTH"),                  $"colour_depth",
        lit("MMS_CAPABLE"),                   $"mms_capable",
        lit("CAMERA_TYPE"),                   $"camera_type",
        lit("CAMERA_RESOLUTION"),             $"camera_resolution",
        lit("VIDEO_MESSAGING_CAPABLE"),       $"video_messaging_capable",
        lit("VIDEO_RECORD"),                  $"video_record",
        lit("RINGTONE_TYPE"),                 $"ringtone_type",
        lit("JAVA_CAPABLE"),                  $"java_capable",
        lit("EMAIL_CLIENT"),                  $"email_client",
        lit("EMAIL_PUSH_CAPABLE"),            $"email_push_capable",
        lit("OPERATING_SYSTEM"),              $"operating_system",
        lit("GOLDEN_GATE_USER_INTERFACE"),    $"golden_gate_user_interface",
        lit("TZONES_HARD_KEY"),               $"tzones_hard_key",
        lit("BLUETOOTH_CAPABLE"),             $"bluetooth_capable".substr(0, 3),
        lit("TM3_CAPABLE"),                   $"tm3_capable",
        lit("WNW_DEVICE"),                    $"wnw_device",
        lit("CONCEPT_CLASS"),                 $"concept_class",
        lit("PRICE_TIER"),                    $"price_tier",
        lit("INTEGRATED_MUSIC_PLAYER"),       $"integrated_music_player",
        lit("GPS"),                           $"gps",
        lit("WNW_BROWSER_CLASS"),             $"wnw_browser_class",
        lit("BROWSER_TYPE"),                  $"browser_type",
        lit("INPUT_METHOD"),                  $"input_method",
        lit("RESOLUTION_MAIN"),               $"resolution_main",
        lit("DISPLAY_SIZE"),                  $"display_size",
        lit("HIGHEST_UPLOAD"),                $"highest_upload",
        lit("HIGHEST_DOWNLOAD"),              $"highest_download",
        lit("LTE"),                           $"lte",
        lit("DISPLAY_TYPE"),                  $"display_type",
        lit("FORM_FACTOR_DETAILED"),          $"form_factor_detailed",
        lit("OPERATING_SYSTEM_DETAILED"),     $"operating_system_detailed",
        lit("OPERATING_SYSTEM_VERSION"),      $"operating_system_version",
        lit("BROWSER_VENDOR"),                $"browser_vendor",
        lit("BROWSER_VERSION"),               $"browser_version",
        lit("BROWSER_VERSION_CAT"),           $"browser_version_cat",
        lit("APP_STORE"),                     $"app_store",
        lit("MVOIP_POSSIBLE_DEVICE"),         $"mvoip_possible_device",
        lit("TM_SMARTPHONE_ALL"),             $"tm_smartphone_all",
        lit("NFC_CAPABLE"),                   $"nfc_capable",
        lit("CPU_POWER"),                     $"cpu_power",
        lit("RAM"),                           $"ram",
        lit("MASTER_TERMINAL_ID"),            $"master_terminal_id",
        lit("NOTICE_FREETEXT"),               $"notice_freetext",
        lit("VoLTE_Capability"),              $"volte_capability"
      ))
      .select($"terminal_id", explode($"specs")).sort("terminal_id", "key")
    .withColumn("value",
      when($"key" === "LAUNCH_DATE", date_format(to_date($"value", "yyyy-MM-dd"),"dd.MM.yyyy HH:mm:ss"))//
        .otherwise($"value"))
    .na.fill("N/A", Seq("value"))
.cache()

    // write normalized output to tmo_terminal_irr_$ODATE.csv
    CSVWriter(data = exploded,
      path = s"${stagePath}tmo_terminal_irr_$ODATE.csv",
      delimiter = "|",
      writeHeader = false,
      escape = "",
      quote = "")
      .writeData()

    val term_spec_update = exploded
      .withColumn("valid_from", lit(formatYMD.format(formatStr.parse(ODATE))))
      .withColumn("valid_to", lit(max_date))
      .withColumn("entry_id", lit(formatInt.format(today)))
      .withColumn("load_date", lit(formatYMDHms.format(today)))
      .withColumnRenamed("key", "terminal_spec_name")
      .withColumnRenamed("value", "terminal_spec_value")

    val lookup_key = Seq("terminal_id" , "terminal_spec_name")
    val schema : Array[String] = d_terminal.columns
exploded.unpersist()
    historize_dim(term_spec_update, d_terminal, schema, lookup_key, ODATE)

  }
}
