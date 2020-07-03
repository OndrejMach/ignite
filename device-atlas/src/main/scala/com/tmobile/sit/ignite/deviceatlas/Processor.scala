package com.tmobile.sit.ignite.deviceatlas

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.{CSVReader, Reader}
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.deviceatlas.config.Setup
import com.tmobile.sit.ignite.deviceatlas.datastructures.FileStructures
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class Inputs(input1: Reader, input2: Reader, input3: Reader)

object Processor extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()


  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  import sparkSession.sqlContext.implicits._

  println("Web UI: " + sparkSession.sparkContext.uiWebUrl)

  val device_map = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\device_map_55476_20200430_pipe.csv",
    header = true,
    schema = Some(FileStructures.deviceMap),
    delimiter = "|"
  )
  val tac_blacklist = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\tac_blacklist.csv",
    header = false,
    schema = Some(FileStructures.tacBlacklist_Lkp)
  )

  val manufacturer_lkp = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\manufacturer.csv",
    header = false,
    schema = Some(FileStructures.manufacturer_Lkp),
    delimiter = "|"
  )

  val manufacturer_vendor_lkp = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\manufacturer_vendor.csv",
    header = false,
    schema = Some(FileStructures.manufacturerVendor_Lkp),
    delimiter = "|"
  )

  val operating_system_lkp = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\operating_system.csv",
    header = false,
    schema = Some(FileStructures.operatingSystem_Lkp),
    delimiter = "|"
  )
  val os_nokia_lkp = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\operating_system_nokia_os.csv",
    header = false,
    schema = Some(FileStructures.osNokia_Lkp),
    delimiter = "|"
  )

  val terminal_db_lkp = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\terminaldb.csv",
    header = false,
    schema = Some(FileStructures.terminalDB_full_lkp),
    delimiter = "|"
  )

  val historical_terminal_db = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\terminal_database_export.csv",
    header = false,
    schema = Some(FileStructures.terminalDB_full_lkp),
    delimiter = "|"
  )

  val terminal_id_lkp = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\terminadb_terminal_id.hwm",
    header = false,
    schema = Some(FileStructures.terminal_id_lkp)
  )

  val extra_terminalDB_records = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\extra_in_terminaldb.csv",
    header = false,
    schema = Some(FileStructures.terminalDB_full_lkp),
    delimiter = "|"
  )

  val device_map_df               = device_map.read()

  val tac_blacklist_df            = tac_blacklist.read()
  val manufacturer_lkp_df         = manufacturer_lkp.read()
  val manufacturer_vendor_lkp_df  = manufacturer_vendor_lkp.read()
  val operating_system_lkp_df     = operating_system_lkp.read()
  val os_nokia_lkp_df             = os_nokia_lkp.read()
  val terminal_db_lkp_df          = terminal_db_lkp.read()
  val extra_terminalDB_records_df = extra_terminalDB_records.read()

  val historical_terminal_db_df   = historical_terminal_db.read()
  val terminal_id_lkp_df = terminal_id_lkp.read()


  println("input file lines: " + device_map_df.count())
  println("blacklist lines: " + tac_blacklist_df.count())
  println("terminal DB lkp lines: " + terminal_db_lkp_df.count())
  println("extra terminal records lines: " + extra_terminalDB_records_df.count())
  println("historical terminal DB lines: " + historical_terminal_db_df.count())
  println("last terminal_id used: " + terminal_id_lkp_df.first().toString())


  //device_map_df uniq on "tac"
  // split ^ to NEW and OLD: [done after all fields are fixed] // line 253 in all_in_one_CODE.txt
  // drop records with "tac" = tac_blacklist_df.tac
      // strip double quotes?
      // if standardised_device_vendor!= null: lookup: gsma_manufacturer, standardised_device_vendor vs. "gsma_manufacturer,gsma_standardised_device_vendor" (1st and 2nd col) of manufacturer_vendor_lkp_df
                                              // match: fixed_manufacturer = terminal_db_manufacturer (3rd column)
      // else  fixed_manufacturer = lookup gsma_manufacturer vs. manufacturer_lkp_df.gsma_manufacturer, return manufacturer_lkp_df.terminal_db_manufacturer
      // if fixed_manufacturer still empty: fixed_manufacturer = gsma_manufacturer
  // if standardised_full_name not null: terminal_full_name = standardised_full_name     [terminal_full_name is a new column]
     // else: terminal_full_name = fixed_manufacturer + " " + gsma_marketing_name
  // if standardised_marketing_name not null: model = standardised_marketing_name       [model is a new column]
     // else: model = gsma_marketing_name
//TODO: check that correct columns are selected
  val device_map_clean = device_map_df
      .select(
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
    .sort("tac", "deviceatlas_id")
    .dropDuplicates("tac")
    .join(tac_blacklist_df, device_map_df("tac") === tac_blacklist_df("tac"), "leftanti")
    .sort("tac")
    .join(manufacturer_vendor_lkp_df
      .select('gsma_manufacturer as "lkp_gsma_manufacturer",
        'gsma_standardised_device_vendor as "lkp_device_vendor",
        'terminal_db_manufacturer as "lkp_manufacturer_vendor"),
      $"gsma_manufacturer" === $"lkp_gsma_manufacturer" &&
        $"standardised_device_vendor" === $"lkp_device_vendor",
      "left_outer")
    .drop("lkp_gsma_manufacturer", "lkp_device_vendor")
    .join(manufacturer_lkp_df.select('gsma_manufacturer as "lkp_gsma_manufacturer", 'terminal_db_manufacturer as "lkp_manufacturer"),
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
      when(col("standardised_full_name").isNull, col("fixed_manufacturer")+" "+col("gsma_marketing_name"))
        .otherwise(col("standardised_full_name")))
    .withColumn("model",
      when(col("standardised_marketing_name").isNull, col("gsma_marketing_name"))
        .otherwise(col("standardised_marketing_name")))

//   device_map_clean.show(100,false)


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
  .join(operating_system_lkp_df.as("right"),
    $"left.os_name" === $"right.gsma_os_name",
    "left_outer")
  .withColumn("fixed_os_name",
    when(col("right.terminaldb").isNull, col("left.os_name"))
    .otherwise(col("right.terminaldb")))
    .drop("gsma_os_name", "terminaldb")
//device_os_name.show()

val device_os_nokia = device_os_name
    .where("fixed_os_name = \"SERIES X\"")
    .join(os_nokia_lkp_df, $"os_version" === $"gsma_os_version",
      "left_outer")
    .withColumn("fixed_os_name",
      when(col("terminaldb").isNull, col("os_name"))
        .otherwise(col("terminaldb")))
    .drop("gsma_os_version", "terminaldb")
//  device_os_nokia.show()

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

      // lookup device_map_df(uniq).tac vs. terminal_db_lkp_df.tac_code:
        // match: terminal_id = terminal_db_lkp_df.terminal_id // INPUT RECORD goes to OLD stream
        // no match: terminal_id = -1                          // INPUT RECORD goes to NEW stream
 val OLD_device_df = device_map_clean_fixed
          .join(terminal_db_lkp_df.select("tac_code", "terminal_id"),
            $"tac" === $"tac_code", "inner")
          .drop("tac_code")
 val NEW_device_df = device_map_clean_fixed
   .join(terminal_db_lkp_df.select("tac_code"/*, "terminal_id"*/),
     $"tac" === $"tac_code", "left_outer")
   .where("tac_code is NULL")
   .drop("tac_code")
/*
//  println("total: " + device_map_clean.count())
  println("total: " + device_map_clean_fixed.count())
  println("old: " + OLD_device_df.count())
  println("new: " + NEW_device_df.count())
*/

  // got NEW_df and OLD_df

  var max_id : Int = terminal_id_lkp_df.first().getInt(0)
//println("old max id: " + max_id)

  // dedup NEW_df by "fixed_manufacturer", "model"
    // assign new terminal_id to each record. Last value in terminadb_terminal_id.hwm file
    // assign same terminal_id to duplicates with same "fixed_manufacturer", "model"
  val NEW_device_ids = NEW_device_df.select("fixed_manufacturer", "model").dropDuplicates("fixed_manufacturer", "model")
    .withColumn("row_nr", row_number.over(Window.orderBy("fixed_manufacturer", "model")))
    .withColumn("terminal_id", expr(s"${max_id} + row_nr"))
    .drop("row_nr")

  max_id += NEW_device_ids.count().toInt

  val NEW_device_withIDs = NEW_device_df.join(NEW_device_ids, Seq("fixed_manufacturer", "model"),
  "left_outer")

  // concat OLD_df with NEW (with new terminal_ids) (and sort by "tac" column) ['left' in below join]
  // join above with some historical export file [historical_terminal_db_df] (sorted by "tac_code" column) as follows:   # ="$EVL_PROJECT_STAGE_DIR/terminal_database_export.csv" file from 2018-02-12
    // join on "tac"="tac_code"  # output in terminalDB_full_lkp structure
    // if match use values from historical file [val hist_records = ... ]
    // else [val not_hist_records = ... ]
      // crazy ass mapping, check the source file: EVM_JOIN="$EVL_PROJECT_DIR/evm/join/terminaldb.evm"

  val OLD_and_NEW_device = OLD_device_df.union(NEW_device_withIDs).sort("tac") // TODO: check and fix schema

  val hist_records = OLD_and_NEW_device.select("tac")
    .join(historical_terminal_db_df,
      $"tac" === $"tac_code",
      "inner")
    .drop("tac")

sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

  val not_hist_records = OLD_and_NEW_device.join(historical_terminal_db_df.select('tac_code as "tac_code_tmp"),
    $"tac" === "tac_code_tmp",
    "left_outer")
    .where("tac_code_tmp is NULL")
    .drop("tac_code_tmp")


  def boolToText(inputCol : Column) : Column = {
    when(inputCol === lit("0"), lit("NO"))
      .when(inputCol === lit("1"), lit("YES"))
      .otherwise(lit(""))
  }

  val not_hist_records_2 = not_hist_records
    // populate "gsm_bandwidth" column, only following values from original column counts: "GSM850", "GSM900", "GSM1800", "GSM1900"
    .withColumn("noSpace",  regexp_replace($"gsma_bands", " ", ""))     // remove spaces from original column to fix values like "GSM 850"
    .withColumn("band1", when(locate("GSM850", $"noSpace") > 0, 1).otherwise(0))
    .withColumn("band2",  when(locate("GSM900", $"noSpace") > 0, 1).otherwise(0))
    .withColumn("band3",  when(locate("GSM1800", $"noSpace") > 0, 1).otherwise(0))
    .withColumn("band4",  when(locate("GSM1900", $"noSpace") > 0, 1).otherwise(0))
    .withColumn("bandsINT", $"band1"+$"band2"+$"band3"+$"band4")
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
      when($"gsma_allocation_date".isNotNull, $"gsma_allocation_date")
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
          .otherwise(upper(substring(regexp_replace($"primary_hardware_type", " ", ""),0,9)))
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
        ($"os_name" isin ("Nucleus", "Other", "Brew", "Enea OSE", "L4","Rex", "Samsung proprietary", "Sony Ericsson proprietary", "LG proprietary", "VRTX")) ||
        (($"operating_system_version" isin ("LG proprietary", "Series 20", "Series 30")) || locate("Series 40", $"operating_system_version") > 0),
       lit("NO"))
      .otherwise("YES"))

    .withColumn("id", $"terminal_id")
    .withColumn("terminal_full_name", trim($"terminal_full_name"))
    .withColumn("browser_type", trim(concat_ws(" ", $"browser_name", $"browser_version")))
    .withColumn("form_factor_detailed", $"form_factor")
    .withColumn("operating_system_detailed", $"os_name")
    .withColumn("browser_version", $"browser_type")

    .withColumn("gprs_capable",     boolToText($"gprs"))
    .withColumn("edge_capable",     boolToText($"edge"))
    .withColumn("umts_capable",     boolToText($"umts"))
    .withColumn("wlan_capable",     boolToText($"wi_fi"))
    .withColumn("java_capable",     boolToText($"js_support_basic_java_script"))
    .withColumn("lte",              boolToText($"lte"))
    .withColumn("nfc_capable",      boolToText($"nfc"))
    .withColumn("volte_capability", boolToText($"volte"))

    .withColumnRenamed("tac", "tac_code")
    .withColumnRenamed("fixed_manufacturer", "manufacturer")
    .withColumnRenamed("screen_color_depth", "colour_depth")
    .withColumnRenamed("camera", "camera_resolution")
    .withColumnRenamed("fixed_os_name","operating_system")
    .withColumnRenamed("total_ram","ram")

    .withColumn("csso_alias",                 lit(""))
    .withColumn("status",                     lit("ACTIVE"))
    .withColumn("international_material_number", lit(""))
    .withColumn("handset_tier",               lit("NONE"))
    .withColumn("wap_type",                   lit("NONE"))
    .withColumn("wap_push_capable",           lit("NO"))
    .withColumn("mms_capable",                lit(""))
    .withColumn("camera_type",                lit("NONE"))
    .withColumn("video_messaging_capable",    lit(""))
    .withColumn("video_record",               lit(""))
    .withColumn("ringtone_type",              lit("NONE"))
    .withColumn("email_client",               lit(""))
    .withColumn("email_push_capable",         lit(""))
    .withColumn("golden_gate_user_interface", lit("NO"))
    .withColumn("tzones_hard_key",            lit("NO"))
    .withColumn("tm3_capable",                lit("NO"))
    .withColumn("wnw_device",                 lit("N/A"))
    .withColumn("concept_class",              lit("N/A"))
    .withColumn("price_tier",                 lit("N/A"))
    .withColumn("integrated_music_player",    lit("N/A"))
    .withColumn("gps",                        lit("N/A"))
    .withColumn("wnw_browser_class",          lit("N/A"))
    .withColumn("input_method",               lit("N/A"))
    .withColumn("display_size",               lit("N/A"))
    .withColumn("highest_upload",             lit("N/A"))
    .withColumn("highest_download",           lit("N/A"))
    .withColumn("display_type",               lit("N/A"))
    .withColumn("browser_vendor",             lit("N/A"))
    .withColumn("browser_version_cat",        lit("N/A"))
    .withColumn("app_store",                  lit("N/A"))
    .withColumn("mvoip_possible_device",      lit("N/A"))
    .withColumn("cpu_power",                  lit("N/A"))
    .withColumn("master_terminal_id",         lit("N/A"))
    .withColumn("notice_freetext",            lit("N/A"))

//  hist_records.printSchema()
//  not_hist_records.printSchema()
//  not_hist_records_2.printSchema()

  val tmp = not_hist_records_2.select(
    "tac_code",
    "id",
    "terminal_id",
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
    "tm3_capable",
    "wnw_device",
    "concept_class",
    "price_tier",
    "integrated_music_player",
    "gps",
    "wnw_browser_class",
    "browser_type",
    "input_method",
    "resolution_main",
    "display_size",
    "highest_upload",
    "highest_download",
    "lte",
    "display_type",
    "form_factor_detailed",
    "operating_system_detailed",
    "operating_system_version",
    "browser_vendor",
    "browser_version",
    "browser_version_cat",
    "app_store",
    "mvoip_possible_device",
    "tm_smartphone_all",
    "nfc_capable",
    "cpu_power",
    "ram",
    "master_terminal_id",
    "notice_freetext",
    "volte_capability").union(hist_records).union(extra_terminalDB_records_df)  // TODO: check and fix schema


    CSVWriter(data = tmp, path = "d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\outs\\test\\terminalDB_DATE.csv", delimiter = "|", writeHeader = true).writeData() // TODO change header to false


  // Merge[=EVL command] extra_in_terminaldb.csv with join output on "tac_code" (stream X)# does it deduplicate or not?

  // write merged output (stream X) to "$EVL_PROJECT_STAGE_DIR/terminaldb_$ODATE.csv" ==> first output file
  //=================== using terminal db = first output file ============================//
  // unique sort merged output (stream X) on "terminal_id"
    // split (normalize?) each row to many, one for each "terminal_spec_name" value
  // write normalized output to tmo_terminal_irr_$ODATE.csv

  // "map" merged output (stream X) = trim some columns (manufacturer(50), model(50), model_alias(255), terminal_full_name(200)
  // write trimmed output to tmo_tac_irr_$ODATE.csv

  // "map" trimmed output: (to cptm_ta_d_tac.evd format):
        // if launch_date != 1900-01-01 use it else launch_date = null
        // set valid_from = $ODATE        yyyy-mm-dd
        // set valid_to = MAX_DATE (4712-12-31)
        // load_date = load_date = $(date "+%Y-%m-%d %H:%M:%S")
        // entry_id = run_id : unique job id, for EVL stored in /data/jobs/evl/prod/ewhr/log/common/evl_run_id.hwm (one in each project dir)
    // sort by tac_code (TAC_SORT flow)
    // historize dimension cptm_ta_d_tac.csv:
        // read old file: hdfs:///data/ewhr/raw/stage/common/cptm_ta_d_tac.csv
        // join old file with TAC_SORT on "tac_code": (old file is left)
            // if no match or left.valid_to < MAX_DATE (4712-12-31) --> left to output === old or closed records from old file
            // else if (!left) right_to_out(); = new records from TAC_SORT
            // else compare all fields except (valid_from, valid_to, entry_id, load_date) if they differ hist_flag =1
              // if hist_flag:
                  // old record: set valid_to=new.valid_from and output it
                  // output new record
              // else use old record
        // write joined output to temp file (cptm_ta_d_tac.tmp)
        // cp cptm_ta_d_tac.csv cptm_ta_d_tac.csv.previous
        // mv cptm_ta_d_tac.tmp cptm_ta_d_tac.csv

  // read tmo_terminal_irr_$ODATE.csv
    // add valid_from = $ODATE        yyyy-mm-dd
    // add valid_to = MAX_DATE (4712-12-31)
    // load_date
    // entry_id

  //  historize dimension cptm_ta_d_terminal_spec.csv:
      // read old file: hdfs:///data/ewhr/raw/stage/common/cptm_ta_d_terminal_spec.csv
      // join old file with  terminal from step above on "terminal_id,terminal_spec_name"
          // if no match or left.valid_to< MAX_DATE --> left to output
          // else if (!left) right_to_out(); = new records from terminal_irr file
          //  SAME AS ABOVE up to 'mv cptm_ta_d_terminal_spec.csv.tmp cptm_ta_d_terminal_spec.csv'


  // backup $EVL_PROJECT_STAGE_DIR/terminaldb.csv to terminaldb_$ODATE.csv.previous
  // first output file ("$EVL_PROJECT_STAGE_DIR/terminaldb_$ODATE.csv") to terminaldb.csv

  // archive input file

}


object Processor_2 extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()

  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  import sparkSession.sqlContext.implicits._

  println("Web UI: " + sparkSession.sparkContext.uiWebUrl)
// TODO change to new output file
  val terminalDB = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\lkp_files\\terminaldb.csv",
    header = false,
    schema = Some(FileStructures.terminalDB_full_lkp),
    delimiter = "|"
  )

  val cptm_ta_d_tac = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\cptm_ta_d_tac.csv",
    header = false,
    schema = Some(FileStructures.cptm_ta_d_tac),
    delimiter = "|"
  )

  val terminalDB_df = terminalDB.read()
  val cptm_ta_d_tac_df = cptm_ta_d_tac.read()

//  println(terminalDB_df.first().schema.fieldNames.toSeq.toString())

  // unique sort merged output (stream X) on "terminal_id"
    // split (normalize?) each row to many, one for each "terminal_spec_name" value

/* // ====== marne pokusy ============
  //  val mapka : Map[String, String] = Map("1" -> "a", "2" -> "b")
//  val seqka : Seq[String] = Seq("a", "b", "1")
//  var seqmapka : Seq[Map[String, String]] = Seq(mapka, mapka)
//  var tmp: Seq[Map[String, String]] = Seq.empty[Map[String, String]]
  var reformat: Seq[(String, Map[String, String])] = Seq.empty[(String, Map[String, String])]

  //terminalDB_df.dropDuplicates("terminal_id").foreach(row => tmp = tmp :+ row.getValuesMap[String](row.schema.fieldNames))
//  terminalDB_df.dropDuplicates("terminal_id").foreach(row =>  println((row.get(2), row.getValuesMap[String](row.schema.fieldNames)).toString()))
//  terminalDB_df.dropDuplicates("terminal_id").foreach(row =>  reformat = reformat:+(row.get(2).toString, row.getValuesMap[String](row.schema.fieldNames)))

  //terminalDB_df.dropDuplicates("terminal_id").foreach(row => println(row.getValuesMap[Any](row.schema.fieldNames)))
  //println(terminalDB_df.first().getValuesMap[Any](terminalDB_df.first().schema.fieldNames).toString())
// reformat.foreach(r => println(r._1 + " | " +  r._2.toString()))
//  reformat_df.show(false)
  //tmp.foreach(m => m.foreach(v => println(s"piece of map: $v")))

  seqmapka = seqmapka:+mapka
  mapka.foreach(v => println(s"XXXXXX ${v._1} : ${v._2}"))
  seqka.foreach(s => println(s"seq elem: $s"))
  seqmapka.foreach(M => M.foreach(s => println(s"seqmap key: ${s._1} value: ${s._2}")))

  //terminalDB_df.select(struct("tac_code", "id", "terminal_id")).show()
  //val allInOne = terminalDB_df.select(struct(terminalDB_df.columns.map(terminalDB_df(_)) : _*))
  //allInOne.withColumnRenamed(allInOne.columns.head, "allInOne").show(false)
*/
  val exploded =  terminalDB_df.dropDuplicates("terminal_id")
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
    lit("BLUETOOTH_CAPABLE"),             $"bluetooth_capable",
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
//  exploded.show(100, false)
// TODO:  bluetooth_capable.getItem(0).toString().substring(0,3)

  // write normalized output to tmo_terminal_irr_$ODATE.csv
  CSVWriter(data = exploded, path = "d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\outs\\test\\tmo_terminal_irr_DATE.csv", delimiter = ";", writeHeader = false).writeData()



  // "map" merged output (stream X) = trim some columns (manufacturer(50), model(50), model_alias(255), terminal_full_name(200)
  // write trimmed output to tmo_tac_irr_$ODATE.csv
  val tac_data = terminalDB_df
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
    .withColumn("model_alias", substring($"model_alias",0 , 50))
    .withColumn("terminal_full_name", substring($"terminal_full_name",0 , 50))
    //.show(false)

    CSVWriter(data = tac_data, path = "d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\outs\\test\\tmo_tac_irr_DATE.csv", delimiter = "|", writeHeader = false).writeData()

  // "map" trimmed output: (to cptm_ta_d_tac.evd format):
        // if launch_date != 1900-01-01 use it else launch_date = null
        // set valid_from = $ODATE        yyyy-mm-dd
        // set valid_to = MAX_DATE (4712-12-31)
        // load_date = load_date = $(date "+%Y-%m-%d %H:%M:%S")
        // entry_id = run_id : unique job id, for EVL stored in /data/jobs/evl/prod/ewhr/log/common/evl_run_id.hwm (one in each project dir)
    // sort by tac_code (TAC_SORT flow)
    // historize dimension cptm_ta_d_tac.csv:
        // read old file: hdfs:///data/ewhr/raw/stage/common/cptm_ta_d_tac.csv
        // join old file with TAC_SORT on "tac_code": (old file is left)
            // if no match or left.valid_to < MAX_DATE (4712-12-31) --> left to output === old or closed records from old file
            // else if (!left) right_to_out(); = new records from TAC_SORT
            // else compare all fields except (valid_from, valid_to, entry_id, load_date) if they differ hist_flag =1
              // if hist_flag:
                  // old record: set valid_to=new.valid_from and output it
                  // output new record
              // else use old record
        // write joined output to temp file (cptm_ta_d_tac.tmp)
        // cp cptm_ta_d_tac.csv cptm_ta_d_tac.csv.previous
        // mv cptm_ta_d_tac.tmp cptm_ta_d_tac.csv
  val ODATE = Calendar.getInstance().getTime() // TODO: proper value
  val today = Calendar.getInstance().getTime()
  val max_date = "4712-12-31"

  val formatYMD = new SimpleDateFormat("yyyy-MM-dd")
  val formatYMDHms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val tac_data_sorted = tac_data.select(
    "terminal_id",
    "tac_code",
    "id",
    "manufacturer",
    "model",
    "model_alias",
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
    "ringtone_type",
    "java_capable",
    "email_client",
    "email_push_capable",
    "operating_system",
    "golden_gate_user_interface",
    "tzones_hard_key",
    "bluetooth_capable",
    "tm3_capable",
    "terminal_full_name",
    "video_record"
    )
    .withColumn("launch_date",
      when($"launch_date" === "1900-01-01", "")
        .otherwise($"launch_date"))
    .withColumn("valid_from", lit(formatYMD.format(ODATE)))
    .withColumn("valid_to", lit(max_date))
    .withColumn("entry_id", lit(998877)) // TODO: proper value
    .withColumn("load_date", lit(formatYMDHms.format(today)))
    .sort("tac_code")

  println("update file line count: " + tac_data_sorted.count())
  println("dimension line count: " + cptm_ta_d_tac_df.count())

  val schema : Array[String] = cptm_ta_d_tac_df.columns

  // get closed
  val closed_records = cptm_ta_d_tac_df.where(s"valid_to != '${max_date}'")
  println("closed records in dimension: " + closed_records.count())

  // join to get unchanged
  val unchanged_records = cptm_ta_d_tac_df.where(s"valid_to = '${max_date}'").join(tac_data_sorted, Seq("tac_code"), "leftanti")
    .select(schema.head, schema.tail:_*)
  println("dimension unchanged records: " + unchanged_records.count())

  // list of columns to use for comparing records
  val compare_columns = cptm_ta_d_tac_df.first().schema.fieldNames.toSeq.filter(_ != "valid_from").filter(_ != "valid_to").filter(_ != "entry_id").filter(_ != "load_date")
  println("columns to compare : "+compare_columns.toString())

  // join to get updates - records present in both tables
  val possible_updates = tac_data_sorted.join(cptm_ta_d_tac_df.where(s"valid_to = '${max_date}'").select("tac_code"), Seq("tac_code"), "inner")
      .drop($"cptm_ta_d_tad_df.tac_code")
  println("possible updates: "+possible_updates.count())
//  possible_updates.show()

  // test all 'compare_columns' to identify unchanged records
  val no_update = cptm_ta_d_tac_df.na.fill("").where(s"valid_to = '${max_date}'")
    .join(possible_updates.drop("valid_from", "valid_to", "load_date", "entry_id").na.fill(""), compare_columns, "inner")
      .select(cptm_ta_d_tac_df.columns.head, cptm_ta_d_tac_df.columns.tail:_*)
println("same records count: " + no_update.count())
//  no_update.show()


  // remove unchanged (no_update) records from possible updates. Possible updates = input file values ==> inserts to DB
  val update_inserts = possible_updates.join(no_update, Seq("tac_code"), "leftanti").select(possible_updates.columns.head, possible_updates.columns.tail:_*)
    .select(schema.head, schema.tail:_*)
  // select existing DB records and update valid_to column
  val update_updates = cptm_ta_d_tac_df.where(s"valid_to = '${max_date}'")
    .join(update_inserts.select("tac_code"), Seq("tac_code"), "inner")
    .drop("$update_inserts.tac_code")
    .withColumn("valid_to", lit(formatYMD.format(ODATE)))
    .select(schema.head, schema.tail:_*)

println("update inserts count: " + update_inserts.count())
println("update updates count: " + update_updates.count())
//  update_updates.sort("terminal_id").show(5, false)
//  update_inserts.sort("terminal_id").show(5, false)

  // join to get new
 val new_records = tac_data_sorted.join(cptm_ta_d_tac_df.where(s"valid_to = '${max_date}'"), Seq("tac_code"), "leftanti")
     .select(schema.head, schema.tail:_*)
  new_records.show()
  println("new records: " + new_records.count())

  new_records.printSchema()
  update_updates.printSchema()
  update_inserts.printSchema()
  unchanged_records.printSchema()
  closed_records.printSchema()
  no_update.printSchema()

  val cptm_ta_d_tac_new = new_records.union(update_inserts).union(update_updates).union(no_update).union(unchanged_records).union(closed_records)
  cptm_ta_d_tac_new.sort("tac_code", "valid_to").show(false)
  println(cptm_ta_d_tac_new.count())


  CSVWriter(data = cptm_ta_d_tac_new.sort("tac_code", "valid_to"), path = "d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\outs\\test\\cptm_ta_d_tac.tmp", delimiter = "|", writeHeader = false).writeData()

//=====================================================================================================================
//=====================================================================================================================
//=====================================================================================================================

  // read tmo_terminal_irr_$ODATE.csv
  // add valid_from = $ODATE        yyyy-mm-dd
  // add valid_to = MAX_DATE (4712-12-31)
  // load_date
  // entry_id

  //  historize dimension cptm_ta_d_terminal_spec.csv:
  // read old file: hdfs:///data/ewhr/raw/stage/common/cptm_ta_d_terminal_spec.csv
  // join old file with  terminal from step above on "terminal_id,terminal_spec_name"
  // if no match or left.valid_to< MAX_DATE --> left to output
  // else if (!left) right_to_out(); = new records from terminal_irr file
  //  SAME AS ABOVE up to 'mv cptm_ta_d_terminal_spec.csv.tmp cptm_ta_d_terminal_spec.csv'


  // backup $EVL_PROJECT_STAGE_DIR/terminaldb.csv to terminaldb_$ODATE.csv.previous
  // first output file ("$EVL_PROJECT_STAGE_DIR/terminaldb_$ODATE.csv") to terminaldb.csv

  // archive input file

  val term_spec = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\tmo_terminal_irr_20200601.csv", // TODO: date in filename
    header = false,
    schema = Some(FileStructures.term_spec),
    delimiter = "|"
  )

  val cptm_term_spec = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\cptm_ta_d_terminal_spec.csv",
    header = false,
    schema = Some(FileStructures.cptm_term_spec),
    delimiter = "|"
  )

  val term_spec_source_df = term_spec.read()
    .withColumn("valid_from", lit(formatYMD.format(ODATE)))
    .withColumn("valid_to", lit(max_date))
    .withColumn("entry_id", lit(998877)) // TODO: proper value
    .withColumn("load_date", lit(formatYMDHms.format(today)))

  val cptm_ta_d_terminal_spec_df = cptm_term_spec.read()

  // ===================================================================================================================
  println("update file line count: " + term_spec_source_df.count())
  println("dimension line count: " + cptm_ta_d_terminal_spec_df.count())

  val lookup_key = Seq("terminal_id" , "terminal_spec_name")
  val schema2 : Array[String] = cptm_ta_d_terminal_spec_df.columns

  // list of columns to use for comparing records
  val compare_columns2 = cptm_ta_d_terminal_spec_df.first().schema.fieldNames.toSeq
    .filter(_ != "valid_from")
    .filter(_ != "valid_to")
    .filter(_ != "entry_id")
    .filter(_ != "load_date")
  println("columns to compare 2 : "+compare_columns2.toString())

  // get closed
  val closed_records2 = cptm_ta_d_terminal_spec_df.where(s"valid_to != '${max_date}'")
  println("closed records 2 in dimension: " + closed_records2.count())

  val open_records = cptm_ta_d_terminal_spec_df.where(s"valid_to = '${max_date}'")

  // join to get unchanged
  val unchanged_records2 = open_records.join(term_spec_source_df, lookup_key, "leftanti")
    .select(schema2.head, schema2.tail:_*)
  println("dimension unchanged records 2: " + unchanged_records2.count())


  // join to get updates - records present in both tables
  val possible_updates2 = term_spec_source_df.join(open_records.select("terminal_id" , "terminal_spec_name"), lookup_key, "inner")
    .drop("open_records.terminal_id", "open_records.terminal_spec_name")
  println("possible updates2: "+possible_updates2.count())
  //  possible_updates.show()

  // test all 'compare_columns' to identify unchanged records
  val no_update2 = open_records.na.fill("")
    .join(possible_updates2.drop("valid_from", "valid_to", "load_date", "entry_id").na.fill(""), compare_columns2, "inner")
    .select(open_records.columns.head, open_records.columns.tail:_*)
  println("same records count 2: " + no_update2.count())
  //  no_update.show()


  // remove unchanged (no_update) records from possible updates. Possible updates = input file values ==> inserts to DB
  val update_inserts2 = possible_updates2.join(no_update2, lookup_key, "leftanti").select(possible_updates2.columns.head, possible_updates2.columns.tail:_*)
    .select(schema2.head, schema2.tail:_*)
  // select existing DB records and update valid_to column
  val update_updates2 = open_records
    .join(update_inserts2.select("terminal_id" , "terminal_spec_name"), lookup_key, "inner")
    .drop("update_inserts.terminal_id", "update_inserts.terminal_spec_name")
    .withColumn("valid_to", lit(formatYMD.format(ODATE)))
    .select(schema2.head, schema2.tail:_*)

  println("update inserts2 count: " + update_inserts2.count())
  println("update updates2 count: " + update_updates2.count())
  //  update_updates.sort("terminal_id").show(5, false)
  //  update_inserts.sort("terminal_id").show(5, false)

  // join to get new
  val new_records2 = term_spec_source_df.join(open_records, lookup_key, "leftanti")
    .select(schema2.head, schema2.tail:_*)
  new_records2.show()
  println("new records 2: " + new_records2.count())

  new_records2.printSchema()
  update_updates2.printSchema()
  update_inserts2.printSchema()
  unchanged_records2.printSchema()
  closed_records2.printSchema()
  no_update2.printSchema()


  val cptm_ta_d_terminal_spec_df_new = new_records2.union(update_inserts2).union(update_updates2).union(no_update2).union(unchanged_records2).union(closed_records2)
  cptm_ta_d_terminal_spec_df_new.sort("terminal_id", "valid_to").show(false)
  println(cptm_ta_d_terminal_spec_df_new.count())



  CSVWriter(data = cptm_ta_d_terminal_spec_df_new.sort("terminal_id", "valid_to"), path = "d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\outs\\test\\cptm_ta_d_terminal_spec.tmp", delimiter = "|", writeHeader = false).writeData()





}

object Processor_3 extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()
  implicit val sparkSession = getSparkSession(conf.settings.appName.get)


  final val max_date = "4712-12-31"

  val cptm_ta_d_tac = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\cptm_ta_d_tac.csv",
    header = false,
    schema = Some(FileStructures.cptm_ta_d_tac),
    delimiter = "|"
  )

  val d_tac = cptm_ta_d_tac.read()

  val filtered_tac = d_tac
    .where(s"valid_to >= '${max_date}' AND (status == 'ACTIVE' OR (LEFT(status, 2) == 'NO' AND RIGHT(status, 4) == 'INFO'))")
    .select("terminal_id", "tac_code", "id", "manufacturer", "model")
    .withColumn("tac_code6", expr("substr(tac_code, 0, 6)"))
    .where("tac_code != '35730808' AND terminal_id != 9002")

  val tac6_cnt = filtered_tac.dropDuplicates("terminal_id" ,"tac_code6","id","manufacturer","model")
    .groupBy("tac_code6")
    .count()
    .where("count == 1")
    .join(filtered_tac, Seq("tac_code6"), "inner")
    .select("tac_code6", "terminal_id")

  filtered_tac.select("tac_code", "terminal_id").union(tac6_cnt).sort("tac_code", "terminal_id").show()

  val cptm_term_spec = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\cptm_ta_d_terminal_spec.csv",
    header = false,
    schema = Some(FileStructures.cptm_term_spec),
    delimiter = "|"
  )
 val d_terminal_spec = cptm_term_spec.read()

  val filtered_term_spec = d_terminal_spec
    .where(s"valid_to >= '${max_date}' AND NOT(terminal_spec_name == 'LAUNCH_DATE' AND terminal_spec_value == 'N/A') AND terminal_id != 9002")

  filtered_term_spec.selectExpr("'L' as flag", "terminal_id", "REPLACE(terminal_spec_name, ';', ',') as terminal_spec_name", "REPLACE(LEFT(terminal_spec_value, 50), ';', ',') as terminal_spec_value")
    .show

}

object Processor_4 extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()
  implicit val sparkSession = getSparkSession(conf.settings.appName.get)


  final val max_date = "4712-12-31"

  val cptm_ta_d_tac = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\cptm_ta_d_tac.csv",
    header = false,
    schema = Some(FileStructures.cptm_ta_d_tac),
    delimiter = "|"
  )

  val d_tac = cptm_ta_d_tac.read()

  val cptm_term_spec = CSVReader("d:\\dokumenty\\T-Mobile_2020\\EVL_terminal_tac\\stage\\common\\cptm_ta_d_terminal_spec.csv",
    header = false,
    schema = Some(FileStructures.cptm_term_spec),
    delimiter = "|"
  )
  val d_terminal_spec = cptm_term_spec.read()

  println(d_tac.columns.toSeq.head)
  val someV = d_tac.columns.toSeq.tail.map(x=>"input.".concat(x))
  println(someV.getClass)
  println(someV.toString())

  d_tac.join(d_terminal_spec, Seq("terminal_id"), "inner")
    .select(d_tac.columns.toSeq.head, d_tac.columns.toSeq.tail.map(x=>"d_tac.".concat(x)):_*)

    .show()

}


object Processor_5 extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()
  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  import sparkSession.sqlContext.implicits._

  final val max_date = "4712-12-31"
  val lookupPath = "d:\\user\\talend_ewhr\\deviceatlas\\lookups\\"
  val device_map = CSVReader("d:\\user\\talend_ewhr\\deviceatlas\\input\\device_map_55476_20200601_pipe.csv",
    header = true,
    schema = Some(FileStructures.deviceMap),
    delimiter = "|"
  )

  val deviceAtlas = device_map.read()

  val manufacturer = {
    val file = lookupPath + "manufacturer.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.manufacturer_Lkp),
      delimiter = "|")
      .read()
  }

  val manufacturerVendor = {
    val file = lookupPath + "manufacturer_vendor.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.manufacturerVendor_Lkp),
      delimiter = "|")
      .read()
  }
  val tacBlacklist = {
    val file = lookupPath + "tac_blacklist.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.tacBlacklist_Lkp),
      delimiter = "|")
      .read()
  }

  val device_map_clean = deviceAtlas.where("tac == 35954209")
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
    .sort("tac", "deviceatlas_id")
    .dropDuplicates("tac")
    .join(tacBlacklist, deviceAtlas("tac") === tacBlacklist("tac"), "leftanti")
    .sort("tac")
    .join(manufacturerVendor
      .select('gsma_manufacturer as "lkp_gsma_manufacturer",
        'gsma_standardised_device_vendor as "lkp_device_vendor",
        'terminal_db_manufacturer as "lkp_manufacturer_vendor"),
      $"gsma_manufacturer" === $"lkp_gsma_manufacturer" &&
        $"standardised_device_vendor" === $"lkp_device_vendor",
      "left_outer")
 //   .drop("lkp_gsma_manufacturer", "lkp_device_vendor")
    .join(manufacturer.select('gsma_manufacturer as "lkp_gsma_manufacturer1", 'terminal_db_manufacturer as "lkp_manufacturer1"),
      $"gsma_manufacturer" === $"lkp_gsma_manufacturer1",
      "left_outer")
   // .drop("lkp_gsma_manufacturer", "lkp_device_vendor")
    .withColumn("fixed_manufacturer",
      when($"lkp_manufacturer_vendor".isNull,
        when($"lkp_manufacturer1".isNull, $"gsma_manufacturer")
          .otherwise($"lkp_manufacturer1"))
        .otherwise($"lkp_manufacturer_vendor"))
   // .drop("lkp_manufacturer_vendor", "lkp_manufacturer")
    .withColumn("terminal_full_name",
      when(col("standardised_full_name").isNull, concat_ws(" ", $"fixed_manufacturer", $"gsma_marketing_name"))
        .otherwise(lit("shit")))
    .withColumn("model",
      when(col("standardised_marketing_name").isNull, col("gsma_marketing_name"))
        .otherwise(col("standardised_marketing_name")))

  device_map_clean.show(false)

}