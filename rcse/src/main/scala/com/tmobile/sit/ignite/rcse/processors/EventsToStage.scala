package com.tmobile.sit.ignite.rcse.processors

import java.sql.Timestamp

import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.udfs.UDFs
import com.tmobile.sit.ignite.rcse.structures.Terminal
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/*
lookups:
- client: EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_client.csv
- TAC: $EWHR_COMMON_STAGE_DIR/cptm_ta_d_tac.csv
- terminal: $EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_terminal.csv
- terminal_sw: $EWHR_RCSE_STAGE_DIR/cptm_ta_d_rcse_terminal_sw.csv
- input events: TMD_{HcsRcsDwh_m4sxvmvsm6h?,RegAsDwh_Aggregate}_$ODATE.csv
 */


class EventsToStage(settings: Settings, load_date: Timestamp)(implicit sparkSession: SparkSession) extends Processor {

  override def processData(): Unit = {
    // input file reading
    import sparkSession.implicits._

    val inputSchema = StructType(
      Seq(
        StructField("date_id", TimestampType, true),
        StructField("msisdn", LongType, true),
        StructField("imsi", StringType, true),
        StructField("rcse_event_type", StringType, true),
        StructField("rcse_subscribed_status_id", IntegerType, true),
        StructField("rcse_active_status_id", IntegerType, true),
        StructField("rcse_tc_status_id", IntegerType, true),
        StructField("imei", StringType, true),
        StructField("rcse_version", StringType, true),
        StructField("client_vendor", StringType, true),
        StructField("client_version", StringType, true),
        StructField("terminal_vendor", StringType, true),
        StructField("terminal_model", StringType, true),
        StructField("terminal_sw_version", StringType, true)
      )
    )

    val clientSchema = StructType(
      Seq(
        StructField("rcse_client_id", IntegerType, true),
        StructField("rcse_client_vendor_sdesc", StringType, true),
        StructField("rcse_client_vendor_ldesc", StringType, true),
        StructField("rcse_client_version_sdesc", StringType, true),
        StructField("rcse_client_version_ldesc", StringType, true),
        StructField("modification_date", TimestampType, true),
        StructField("entry_id", IntegerType, true),
        StructField("load_date", TimestampType, true)
      )
    )

    val terminalSWSchema = StructType(
      Seq(
        StructField("rcse_terminal_sw_id", IntegerType, true),
        StructField("rcse_terminal_sw_desc", StringType, true),
        StructField("modification_date", TimestampType, true),
        StructField("entry_id", IntegerType, true),
        StructField("load_date", TimestampType, true)

      )
    )


    val data = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/input/TMD_*",
      header = false,
      schema = Some(inputSchema),
      timestampFormat = "yyyyMMddHHmmss",
      delimiter = "|"
    ) read()


    val client = CSVReader(
      path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_client.csv",
      header = false,
      schema = Some(clientSchema),
      timestampFormat = "yyyy-MM-DD HH:mm:ss",
      delimiter = "|"
    ).read()

    val tacTerminal = CSVReader(
      path = settings.tacPath,
      header = false,
      schema = Some(Terminal.tac_struct),
      delimiter = "|"
    ).read()

    val terminal = CSVReader(path = settings.terminalPath,
      header = false,
      schema = Some(Terminal.terminal_d_struct),
      delimiter = "|")
      .read()


    val terminalSW = CSVReader(path = "/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_d_rcse_terminal_sw.csv",
      header = false,
      schema = Some(terminalSWSchema),
      delimiter = "|",
      timestampFormat = "yyyy-MM-DD HH:mm:ss")
      .read()


    val onlyMSISDNS = data.select("msisdn")

    val dmEventsOnly = data.filter($"rcse_event_type" === lit("DM"))
    val regDER = data.filter($"rcse_event_type" =!= lit("DM"))

    println(s"DM: ${dmEventsOnly.count()} REGDER: ${regDER.count()}")

    val encoder3des = udf(UDFs.encode)

    val withLookups = dmEventsOnly
      .withColumn("natco_code", lit("TMD"))
      .withColumn("imsi", when($"imsi".isNotNull, encoder3des(lit(settings.encoderPath), $"imsi")).otherwise($"imsi"))
      .withColumn("tac_code", when($"imei".isNotNull && length($"imei") > lit(8), trim($"imei").substr(0, 8)).otherwise($"imei"))
      .withColumn("client_vendor", upper($"client_vendor"))
      .withColumn("client_vendor", upper($"client_version"))
      .withColumn("client_vendor", upper($"terminal_vendor"))
      .withColumn("client_vendor", upper($"terminal_model"))
      .withColumn("client_vendor", upper($"terminal_sw_version"))
      .join(client.select("rcse_client_id", "rcse_client_vendor_sdesc", "rcse_client_version_sdesc"),
        $"rcse_client_vendor_sdesc" === $"client_vendor" && $"rcse_client_version_sdesc" === $"client_version", "left_outer")
      .drop("rcse_client_vendor_sdesc", "rcse_client_version_sdesc")
      .join(tacTerminal.select("tac_code", "terminal_id"), Seq("tac_code"), "left_outer")
      .join(terminal.select("rcse_terminal_id", "terminal_id"), Seq("terminal_id"), "left_outer")
      .withColumnRenamed("rcse_terminal_id", "rcse_terminal_id_terminal")
      .join(terminal.select("tac_code", "rcse_terminal_id").sort().distinct(), Seq("tac_code"), "left_outer")
      .withColumnRenamed("rcse_terminal_id", "rcse_terminal_id_tac")
      .join(terminal.select("rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc", "rcse_terminal_id"),
        $"terminal_vendor" === $"rcse_terminal_vendor_sdesc" && $"rcse_terminal_model_sdesc" === $"terminal_model", "left_outer")
      .drop("rcse_terminal_vendor_sdesc", "rcse_terminal_model_sdesc")
      .withColumnRenamed("rcse_terminal_id", "rcse_terminal_id_desc")
      .withColumn("rcse_terminal_id",
        when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
          .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
            .otherwise($"rcse_terminal_id_desc")
          )
      )
      .drop("rcse_terminal_id_terminal", "rcse_terminal_id_tac", "rcse_terminal_id_desc")
      .join(terminalSW.select("rcse_terminal_sw_id", "rcse_terminal_sw_desc"), $"terminal_sw_version" === $"rcse_terminal_sw_desc", "left_outer")
      .drop("rcse_terminal_sw_desc")
      .sort("msisdn", "date_id")
      .groupBy("msisdn")
      .agg(
        first("date_id"),
        first("natco_code"),
        first("imsi"),
        first("rcse_event_type"),
        first("rcse_subscribed_status_id"),
        first("rcse_active_status_id"),
        first("rcse_tc_status_id"),
        first("tac_code"),
        first("rcse_version"),
        first("rcse_client_id"),
        first("rcse_terminal_id"),
        first("rcse_terminal_sw_id"),
        first("terminal_id"),
        first("client_vendor"),
        first("client_version"),
        first("terminal_vendor"),
        first("terminal_model"),
        first("terminal_sw_version")
      ).persist()

    withLookups.show(false)

    //println(s"${withLookups.count()}, ${withLookups.distinct().count()}")


    val clientMax = client.select(max("rcse_client_id")).collect()(0).getLong(0)

    // Dimension Client
    val dimensionA =
      withLookups
        .filter($"rcse_client_id".isNotNull)
        .select(
          lit(-1).as("rcse_client_id"),
          $"client_vendor".as("rcse_client_vendor_sdesc"),
          $"client_vendor".as("rcse_client_vendor_ldesc"),
          $"client_version".as("rcse_client_version_sdesc"),
          $"client_version".as("rcse_client_version_ldesc"),
          lit(load_date).as("modification_date"))
        .sort(desc("rcse_client_vendor_sdesc"), desc("rcse_client_version_sdesc"), desc("modification_date"))
        .groupBy("rcse_client_vendor_sdesc", "rcse_client_version_sdesc")
        .agg(
          first("rcse_client_id"),
          first("rcse_client_vendor_ldesc"),
          first("rcse_client_version_ldesc"),
          first("modification_date")
        )
        .withColumn("rcse_client_id", monotonically_increasing_id() + lit(clientMax))

    //Dimension terminal

    val dimensionBOld =
      withLookups
        .filter($"rcse_terminal_id".isNotNull)
        .join(terminal.select($"tac_code".as("tac_code_lkp"), $"terminal_id".as("terminal_id_lkp"), $"rcse_terminal_id"), Seq("rcse_terminal_id"), "left_outer").cache()
        .filter($"tac_code_lkp".isNull && $"terminal_id_lkp".isNull && $"tac_code".isNotNull)
        .select(
          $"rcse_terminal_id",
          $"tac_code",
          lit(null).as("terminal_id"),
          $"terminal_vendor".as("rcse_terminal_vendor_sdesc"),
          $"terminal_vendor".as("rcse_terminal_vendor_ldesc"),
          $"terminal_model".as("rcse_terminal_model_sdesc"),
          $"terminal_model".as("rcse_terminal_model_ldesc"),
          lit(load_date).as("modification_date")
        )

    val dimensionBNew =
      withLookups
        .filter($"rcse_terminal_id".isNull)
        .withColumn("rcse_terminal_id", lit(-1))
        .join(tacTerminal.select("manufacturer", "model", "terminal_id"), Seq("terminal_id"), "left_outer")
        .withColumn("tac_code", when($"terminal_id".isNull, $"tac_code"))
        .withColumn("rcse_terminal_vendor_sdesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
        .withColumn("rcse_terminal_vendor_ldesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
        .withColumn("rcse_terminal_model_sdesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))
      .select(
        lit(-1).as("rcse_terminal_id"),
        $"tac_code",
        $"terminal_id",
        $"rcse_terminal_vendor_sdesc",
        $"rcse_terminal_vendor_ldesc",
        $"rcse_terminal_model_sdesc",
        $"rcse_terminal_model_ldesc",
        lit(load_date).as("modification_date")
      )


  }

}
