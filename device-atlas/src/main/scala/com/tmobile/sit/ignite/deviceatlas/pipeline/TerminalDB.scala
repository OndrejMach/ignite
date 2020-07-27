package com.tmobile.sit.ignite.deviceatlas.pipeline

import java.text.SimpleDateFormat

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.deviceatlas.data.{InputData, LookupData}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait TerminalDBProcessing extends Logger{
  def updateTerminalDB(input: InputData, lookups: LookupData, ODATE: String, output_path: String): DataFrame
}

class TerminalDB (implicit  sparkSession: SparkSession) extends TerminalDBProcessing {

  import sparkSession.sqlContext.implicits._

  def boolToText(inputCol : Column) : Column = {
    when(inputCol === lit("0"), lit("NO"))
      .when(inputCol === lit("1"), lit("YES"))
      .otherwise(lit(""))
  }

  override def updateTerminalDB(input: InputData, lookups: LookupData, ODATE: String, output_path: String): DataFrame = {
    //input.deviceAtlas uniq on "tac"
    // split ^ to NEW and OLD: [done after all fields are fixed]
    // drop records with "tac" = tac_blacklist.tac
    // if standardised_device_vendor!= null: lookup: gsma_manufacturer, standardised_device_vendor vs. "gsma_manufacturer,gsma_standardised_device_vendor"
    // match: fixed_manufacturer = terminal_db_manufacturer (3rd column)
    // else  fixed_manufacturer = lookup gsma_manufacturer vs. manufacturer_lkp_df.gsma_manufacturer, return manufacturer_lkp_df.terminal_db_manufacturer
    // if fixed_manufacturer still empty: fixed_manufacturer = gsma_manufacturer
    // if standardised_full_name not null: terminal_full_name = standardised_full_name     [terminal_full_name is a new column]
    // else: terminal_full_name = fixed_manufacturer + " " + gsma_marketing_name
    // if standardised_marketing_name not null: model = standardised_marketing_name       [model is a new column]
    // else: model = gsma_marketing_name
    logger.info("Updating Terminal-DB data")
    val formatStr = new SimpleDateFormat("dd-MMM-yyyy")
    val formatYMD = new SimpleDateFormat("yyyy-MM-dd")
    val device_map_clean = input.deviceAtlas
      .select(    // selecting only necessary columns
        "tac",
        "gsma_marketing_name",
        "gsma_internal_model_name",
        "gsma_manufacturer",
        "gsma_bands",
        "gsma_allocation_date",
        "gsma_bluetooth",
        "deviceatlas_id",
        "standardised_full_name",
        "standardised_device_vendor",
        "standardised_device_model",
        "standardised_marketing_name",
        "primary_hardware_type",
        "touch_screen",
        "screen_width",
        "screen_height",
        "screen_color_depth",
        "nfc",
        "camera",
        "total_ram",
        "os_name",
        "os_version",
        "browser_name",
        "browser_version",
        "js_support_basic_java_script",
        "gprs",
        "edge",
        "umts",
        "lte",
        "volte",
        "wi_fi")
      .sort($"tac", $"deviceatlas_id".desc)
      .dropDuplicates("tac")
      .join(lookups.tacBlacklist, input.deviceAtlas("tac") === lookups.tacBlacklist("tac"), "leftanti")
      .sort("tac")
      .join(lookups.manufacturerVendor
        .select('gsma_manufacturer as "lkp_gsma_manufacturer",
          'gsma_standardised_device_vendor as "lkp_device_vendor",
          'terminal_db_manufacturer as "lkp_manufacturer_vendor"),
        $"gsma_manufacturer" === $"lkp_gsma_manufacturer" &&
          $"standardised_device_vendor" === $"lkp_device_vendor",
        "left_outer")
      .drop("lkp_gsma_manufacturer", "lkp_device_vendor")
      .join(lookups.manufacturer.select('gsma_manufacturer as "lkp_gsma_manufacturer", 'terminal_db_manufacturer as "lkp_manufacturer"),
        $"gsma_manufacturer" === $"lkp_gsma_manufacturer",
        "left_outer")
      .drop("lkp_gsma_manufacturer", "lkp_device_vendor")
      .withColumn("fixed_manufacturer",
        when($"lkp_manufacturer_vendor".isNull,
          when($"lkp_manufacturer".isNull, $"gsma_manufacturer")
            .otherwise($"lkp_manufacturer"))
          .otherwise($"lkp_manufacturer_vendor"))
      .drop("lkp_manufacturer_vendor", "lkp_manufacturer")
      .withColumn("terminal_full_name",
        when(col("standardised_full_name").isNull, concat_ws(" ", $"fixed_manufacturer", $"gsma_marketing_name"))
          .otherwise(col("standardised_full_name")))
      .withColumn("model",
        when(col("standardised_marketing_name").isNull, col("gsma_marketing_name"))
          .otherwise(col("standardised_marketing_name")))
.cache()

    // if os_name not null: lookup os_name vs. operating_system_lkp_df.gsma_os_name
    // match: fixed_operating_system = operating_system_lkp_df.terminaldb
    // if fixed_operating_system still empty: fixed_operating_system = os_name
    // if fixed_operating_system == "SERIES X" :
    // lookup os_version vs. os_nokia_lkp_df.gsma_os_version:
    // match: fixed_operating_system = os_nokia_lkp_df.terminaldb
    // else:  fixed_operating_system = os_name
    // else: (os_name not null)
    // fixed_operating_system = null

    val device_os_name = device_map_clean.as("left")
      .select("tac", "deviceatlas_id", "os_name", "os_version")
      //.select(device_map_clean.col("os_name"), operating_system_lkp_df.col("terminaldb"))
      .join(lookups.operatingSystem.as("right"),
        $"left.os_name" === $"right.gsma_os_name",
        "left_outer")
      .withColumn("fixed_os_name",
        when(col("right.terminaldb").isNull, col("left.os_name"))
          .otherwise(col("right.terminaldb")))
      .drop("gsma_os_name", "terminaldb")

    val device_os_nokia = device_os_name
      .where("fixed_os_name = \"SERIES X\"")
      .join(lookups.osNokia, $"os_version" === $"gsma_os_version",
        "left_outer")
      .withColumn("fixed_os_name",
        when(col("terminaldb").isNull, col("os_name"))
          .otherwise(col("terminaldb")))
      .drop("gsma_os_version", "terminaldb")

    val device_map_clean_fixed = device_map_clean
      .join(device_os_name.select('tac as "tac_tmp", 'deviceatlas_id as "atlas_id_tmp", 'fixed_os_name),
        $"tac" === $"tac_tmp" && $"deviceatlas_id" === $"atlas_id_tmp",
        "left_outer")
      .drop("tac_tmp", "atlas_id_tmp")
      .join(device_os_nokia.select('tac as "tac_tmp", 'deviceatlas_id as "atlas_id_tmp", 'fixed_os_name as "nokia_os_fix"),
        $"tac" === $"tac_tmp" && $"deviceatlas_id" === $"atlas_id_tmp",
        "left_outer")
      .withColumn("fixed_os_name", when($"nokia_os_fix".isNotNull, $"nokia_os_fix").otherwise($"fixed_os_name"))
      .drop("tac_tmp", "atlas_id_tmp", "nokia_os_fix")

    // Split records to NEW and OLD (as mentioned in the first comment) - lookup device_map.tac(uniq) vs. terminal_db.tac_code:
    // match: terminal_id = terminal_db.terminal_id // INPUT RECORD goes to OLD stream
    // no match: terminal_id = -1                   // INPUT RECORD goes to NEW stream
    val OLD_device_df = device_map_clean_fixed
      .join(lookups.terminalDB.select('tac_code, 'terminal_id, 'manufacturer as "manufacturer_old" ),
        $"tac" === $"tac_code", "inner")
      .drop("tac_code", "fixed_manufacturer")
      .withColumnRenamed("manufacturer_old", "fixed_manufacturer")
    val NEW_device_df = device_map_clean_fixed
      .join(lookups.terminalDB.select("tac_code" /*, "terminal_id"*/),
        $"tac" === $"tac_code", "left_outer")
      .where("tac_code is NULL")
      .drop("tac_code")

    // get last used terminal_id
    var max_id: Int = lookups.terminalId.first().getInt(0)
    logger.info(s"Last terminal_id used: $max_id")

    // dedup NEW_df by "fixed_manufacturer", "model"
    // assign new terminal_id to each record. Last value in terminadb_terminal_id.hwm file
    // assign same terminal_id to duplicates with same "fixed_manufacturer", "model"
    val NEW_device_ids = NEW_device_df.select("fixed_manufacturer", "model")
      .sort("fixed_manufacturer", "model")
      .dropDuplicates("fixed_manufacturer", "model")
      .withColumn("row_nr", row_number.over(Window.orderBy("fixed_manufacturer", "model")))
      .withColumn("terminal_id", expr(s"$max_id + row_nr"))
      .drop("row_nr")

    // update last used terminal_id
    max_id += NEW_device_ids.count().toInt
    CSVWriter(Seq(max_id).toDF("max_id"),
      path = s"${output_path}terminaldb_terminal_id.hwm",
      delimiter = "|",
      writeHeader = false).writeData()
    logger.info(s"New max terminal_id: $max_id")

    val NEW_device_withIDs = NEW_device_df.join(NEW_device_ids, Seq("fixed_manufacturer", "model"),
      "left_outer")

    // concat OLD_df with NEW (with new terminal_ids) (and sort by "tac" column) ['left' in below join]
    // join above with some historical export file [lookups.historical_terminalDB] (sorted by "tac_code" column) as follows:  # ="$EVL_PROJECT_STAGE_DIR/terminal_database_export.csv" file from 2018-02-12
    // join on "tac"="tac_code"  # output in terminalDB_full_lkp structure
    // if match use values from historical file [val hist_records = ... ]
    // else [val not_hist_records = ... ]
    // crazy ass mapping, check the source file: EVM_JOIN="$EVL_PROJECT_DIR/evm/join/terminaldb.evm"

    val OLD_and_NEW_device = OLD_device_df.union(NEW_device_withIDs.select(OLD_device_df.columns.head, OLD_device_df.columns.tail:_*))
      .sort("tac")

    val hist_records = OLD_and_NEW_device.select("tac")
      .join(lookups.historical_terminalDB,
        $"tac" === $"tac_code",
        "inner")
      .drop("tac")

    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

    val not_hist_records = OLD_and_NEW_device.join(lookups.historical_terminalDB.select("tac_code"),
      $"tac" === $"tac_code",
      "left_outer")
      .where("tac_code is NULL")
      .drop("tac_code")

    // START of crazy mapping
    val not_hist_records_mapped = not_hist_records
      // populate "gsm_bandwidth" column, only following values from original column counts: "GSM850", "GSM900", "GSM1800", "GSM1900"
      .withColumn("noSpace", regexp_replace($"gsma_bands", " ", "")) // remove spaces from original column to fix values like "GSM 850"
      .withColumn("band1", when(locate("GSM850", $"noSpace") > 0, 1).otherwise(0))
      .withColumn("band2", when(locate("GSM900", $"noSpace") > 0, 1).otherwise(0))
      .withColumn("band3", when(locate("GSM1800", $"noSpace") > 0, 1).otherwise(0))
      .withColumn("band4", when(locate("GSM1900", $"noSpace") > 0, 1).otherwise(0))
      .withColumn("bandsINT", $"band1" + $"band2" + $"band3" + $"band4")
      .withColumn("gsm_bandwidth",
        when($"bandsINT" === 1, "MONO")
          .when($"bandsINT" === 2, "DUAL")
          .when($"bandsINT" === 3, "TRI")
          .when($"bandsINT" === 4, "QUAD")
          .otherwise("NONE"))
      .drop("noSpace", "band1", "band2", "band3", "band4", "bandsINT")
      // "gsm_bandwidth" DONE

      .withColumn("model_alias",
        when($"standardised_device_model".isNotNull, $"standardised_device_model")
          .otherwise($"gsma_internal_model_name"))

      .withColumn("launch_date",
        when($"gsma_allocation_date".isNotNull, to_date($"gsma_allocation_date", "dd-MMM-yyyy"))
          .otherwise(lit("1900-01-01")))

      .withColumn("form_factor",
        when($"primary_hardware_type".isNotNull,
          when($"primary_hardware_type" === "Mobile Phone" && $"touch_screen" === 1, lit("FANCY"))
            .when($"primary_hardware_type" === "Mobile Phone", "CANDYBAR")
            .when($"primary_hardware_type" === "Wireless Hotspot", "ROUTER")
            .when($"primary_hardware_type" === "Embedded Network Module", "DATACARD")
            .when($"primary_hardware_type" === "Plug-in Modem", "FIXED WIR")
            .when($"primary_hardware_type" === "Payment Terminal" || $"primary_hardware_type" === "Data Collection Terminal", "MACHINE")
            .when($"primary_hardware_type" === "Tablet", "TABLET")
            .when($"primary_hardware_type" === "Wristwatch", "WATCH")
            .otherwise(upper(substring(regexp_replace($"primary_hardware_type", " ", ""), 0, 9)))
        )
          .otherwise("")
      )

      .withColumn("bluetooth_capable",
        when($"gsma_bluetooth" === "Y", lit("YES"))
          .when($"gsma_bluetooth" === "N", lit("NO"))
          .otherwise($"gsma_bluetooth"))

      .withColumn("resolution_main",
        when($"screen_height".isNotNull && $"screen_width".isNotNull, concat_ws("x", $"screen_height", $"screen_width"))
          .otherwise(lit("N/A")))

      .withColumnRenamed("os_version", "operating_system_version")

      .withColumn("tm_smartphone_all",
        when($"os_name".isNull ||
          ($"operating_system_version".isNull && $"os_name" === "Nokia OS") ||
          ($"os_name" isin("Nucleus", "Other", "Brew", "Enea OSE", "L4", "Rex", "Samsung proprietary", "Sony Ericsson proprietary", "LG proprietary", "VRTX")) ||
          (($"operating_system_version" isin("LG proprietary", "Series 20", "Series 30")) || locate("Series 40", $"operating_system_version") > 0),
          lit("NO"))
          .otherwise("YES"))

      .withColumn("id", $"terminal_id")
      .withColumn("terminal_full_name", trim($"terminal_full_name"))
      .withColumn("browser_type", trim(concat_ws(" ", $"browser_name", $"browser_version")))
      .withColumn("form_factor_detailed", $"form_factor")
      .withColumn("operating_system_detailed", $"os_name")
      .withColumn("browser_version", $"browser_type")
      // values '0', '1' to 'NO', 'YES'
      .withColumn("gprs_capable", boolToText($"gprs"))
      .withColumn("edge_capable", boolToText($"edge"))
      .withColumn("umts_capable", boolToText($"umts"))
      .withColumn("wlan_capable", boolToText($"wi_fi"))
      .withColumn("java_capable", boolToText($"js_support_basic_java_script"))
      .withColumn("lte", boolToText($"lte"))
      .withColumn("nfc_capable", boolToText($"nfc"))
      .withColumn("volte_capability", boolToText($"volte"))
      // renamed columns
      .withColumnRenamed("tac", "tac_code")
      .withColumnRenamed("fixed_manufacturer", "manufacturer")
      .withColumnRenamed("screen_color_depth", "colour_depth")
      .withColumnRenamed("camera", "camera_resolution")
      .withColumnRenamed("fixed_os_name", "operating_system")
      .withColumnRenamed("total_ram", "ram")
      // hardcoded values
      .withColumn("csso_alias", lit(""))
      .withColumn("status", lit("ACTIVE"))
      .withColumn("international_material_number", lit(""))
      .withColumn("handset_tier", lit("NONE"))
      .withColumn("wap_type", lit("NONE"))
      .withColumn("wap_push_capable", lit("NO"))
      .withColumn("mms_capable", lit(""))
      .withColumn("camera_type", lit("NONE"))
      .withColumn("video_messaging_capable", lit(""))
      .withColumn("video_record", lit(""))
      .withColumn("ringtone_type", lit("NONE"))
      .withColumn("email_client", lit(""))
      .withColumn("email_push_capable", lit(""))
      .withColumn("golden_gate_user_interface", lit("NO"))
      .withColumn("tzones_hard_key", lit("NO"))
      .withColumn("tm3_capable", lit("NO"))
      .withColumn("wnw_device", lit("N/A"))
      .withColumn("concept_class", lit("N/A"))
      .withColumn("price_tier", lit("N/A"))
      .withColumn("integrated_music_player", lit("N/A"))
      .withColumn("gps", lit("N/A"))
      .withColumn("wnw_browser_class", lit("N/A"))
      .withColumn("input_method", lit("N/A"))
      .withColumn("display_size", lit("N/A"))
      .withColumn("highest_upload", lit("N/A"))
      .withColumn("highest_download", lit("N/A"))
      .withColumn("display_type", lit("N/A"))
      .withColumn("browser_vendor", lit("N/A"))
      .withColumn("browser_version_cat", lit("N/A"))
      .withColumn("app_store", lit("N/A"))
      .withColumn("mvoip_possible_device", lit("N/A"))
      .withColumn("cpu_power", lit("N/A"))
      .withColumn("master_terminal_id", lit("N/A"))
      .withColumn("notice_freetext", lit("N/A"))
    // END of crazy mapping

    // Merge[=EVL command] extra_in_terminaldb.csv with join output on "tac_code" (stream X)
    val new_terminalDB = not_hist_records_mapped
      .select(lookups.terminalDB.columns.head, lookups.terminalDB.columns.tail:_*)
      .union(hist_records)
      .union(lookups.extra_terminalDB_records)

    // write merged output (stream X) to "$EVL_PROJECT_STAGE_DIR/terminaldb_$ODATE.csv" ==> first output file
    /*CSVWriter(new_terminalDB,
      path = s"${output_path}terminaldb_$ODATE.csv",
      delimiter = "|",
      writeHeader = false)
    */

    logger.info("Terminal-DB update DONE...")

    new_terminalDB.sort("tac_code").dropDuplicates("tac_code")
  }
}
