package com.tmobile.sit.ignite.rcse.processors

import java.sql.Timestamp

import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.processors.datastructures.EventsStage
import com.tmobile.sit.ignite.rcse.processors.events.EventsInputData
import com.tmobile.sit.ignite.rcse.processors.udfs.UDFs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SaveMode, SparkSession}

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

    val inputData = new EventsInputData(settings)

    val onlyMSISDNS = inputData.dataInput.select("msisdn")

    val dmEventsOnly = inputData.dataInput.filter($"rcse_event_type" === lit("DM"))
    val regDER = inputData.dataInput.filter($"rcse_event_type" =!= lit("DM"))

    //println(s"DM: ${dmEventsOnly.count()} REGDER: ${regDER.count()}")

    val encoder3des = udf(UDFs.encode)


    val withLookups = dmEventsOnly
      .withColumn("natco_code", lit("TMD"))
      //.withColumn("imsi", when($"imsi".isNotNull, encoder3des(lit(settings.encoderPath), $"imsi")).otherwise($"imsi"))
      .join(inputData.imsi3DesLookup, $"imsi" === $"number", "left_outer")
      .withColumn("imsi", $"des")
      .na.fill("#", Seq("msisdn"))
      .drop("des", "number")
      .withColumn("tac_code", when($"imei".isNotNull && length($"imei") > lit(8), trim($"imei").substr(0, 8)).otherwise($"imei"))
      .withColumn("client_vendor", upper($"client_vendor"))
      .withColumn("client_version", upper($"client_version"))
      .withColumn("terminal_vendor", upper($"terminal_vendor"))
      .withColumn("terminal_model", upper($"terminal_model"))
      .withColumn("terminal_sw_version", upper($"terminal_sw_version"))
      .tacLookup(inputData.tac)
      .clientLookup(inputData.client)
      .terminalLookup(inputData.terminal)
      .withColumn("rcse_terminal_id",
        when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
          .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
            .otherwise($"rcse_terminal_id_desc")
          )
      )
      .drop("rcse_terminal_id_terminal", "rcse_terminal_id_tac", "rcse_terminal_id_desc")
      .terminalSWLookup(inputData.terminalSW)
      .sort(asc("msisdn"), asc("date_id"))
      .groupBy("msisdn")
      .agg(
        last("date_id").alias("date_id"),
        (for (i <- EventsStage.withLookups if i != "msisdn" && i != "date_id" ) yield {
          last(i).alias(i)
        }): _*
      ).persist()

    logger.info(s"Input files enriched, row count is: ${withLookups.count()}")

    //withLookups.filter("rcse_terminal_id is null").show(false)

    //println(s"${withLookups.count()}, ${withLookups.distinct().count()}")


    val clientMax = inputData.client.select(max("rcse_client_id")).collect()(0).getInt(0)

    // Dimension Client
    val dimensionA =
      withLookups
        .filter($"rcse_client_id".isNull)
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
          first("rcse_client_id").alias("rcse_client_id"),
          first("rcse_client_vendor_ldesc").alias("rcse_client_vendor_ldesc"),
          first("rcse_client_version_ldesc").alias("rcse_client_version_ldesc"),
          first("modification_date").alias("modification_date")
        )
        .withColumn("rcse_client_id", monotonically_increasing_id() + lit(clientMax))


    logger.info(s"Updating client dimension, count: ${dimensionA.count()}")

    //Dimension terminal

    val dimensionBOld =
      withLookups
        .filter($"rcse_terminal_id".isNotNull)
        .join(
          inputData.terminal.select($"tac_code".as("tac_code_lkp"), $"terminal_id".as("terminal_id_lkp"), $"rcse_terminal_id"), Seq("rcse_terminal_id"), "left_outer").cache()
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
        .join(inputData.tac.select("manufacturer", "model", "terminal_id"), Seq("terminal_id"), "left_outer")
        .withColumn("tac_code", when($"terminal_id".isNull, $"tac_code"))
        .withColumn("rcse_terminal_vendor_sdesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
        .withColumn("rcse_terminal_vendor_ldesc", when($"terminal_id".isNotNull, $"manufacturer").otherwise($"terminal_vendor"))
        .withColumn("rcse_terminal_model_sdesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))
        .withColumn("rcse_terminal_model_ldesc", when($"terminal_id".isNotNull, $"model").otherwise($"terminal_model"))
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

    logger.info(s"Updating terminal dimension, new rows: ${dimensionBNew.count()}")

    //DIMENSION C
    val maxTerminalSWId = inputData.terminalSW.select(max("rcse_terminal_sw_id")).collect()(0).getInt(0)
    val dimensionC = withLookups
      .filter($"rcse_terminal_sw_id".isNull)
      .select(
        lit(-1).as("rcse_terminal_sw_id"),
        $"terminal_sw_version".as("rcse_terminal_sw_desc"),
        lit(load_date).as("modification_date")
      )
      .sort(desc("rcse_terminal_sw_desc"), desc("modification_date"))
      .groupBy("rcse_terminal_sw_desc")
      .agg(
        first("rcse_terminal_sw_id").alias("rcse_terminal_sw_id"),
        first("modification_date").alias("modification_date")
      )
      .withColumn("rcse_terminal_sw_id", monotonically_increasing_id() + lit(maxTerminalSWId))

    logger.info(s"Updating dimension Terminal SW version, new rows count: ${dimensionC.count()}")

    //DIMENSION D
    val dimensionD = withLookups
      .select(
        EventsStage.withLookups.head, EventsStage.withLookups.tail: _*
      )

    val nonDM = regDER
      .sort(asc("msisdn"), asc("rcse_event_type"), asc("date_id"))
      .groupBy(asc("msisdn"), asc("rcse_event_type"))
      .agg(
        last("date_id").alias("date_id"),
        (for (i <- EventsStage.input if i != "msisdn" && i != "rcse_event_type" && i != "date_id") yield {
          last(i).alias(i)
        }): _*
      )
      .withColumn("tac_code", when($"imei".isNotNull && length($"imei") > lit(8), trim($"imei").substr(0, 8)).otherwise($"imei"))
      .withColumn("date_id", $"date_id".cast(DateType))
      .withColumn("natco_code", lit("TMD"))
      .withColumn("msisdn", when($"msisdn".isNotNull, $"msisdn").otherwise(lit("#")))
      .join(inputData.msisdn3DesLookup, $"msisdn" === $"number", "left_outer")
      .withColumn("msisdn", $"des")
      .drop("des", "number")
      // .withColumn("imsi", when($"imsi".isNotNull, encoder3des($"imsi")))
      .join(inputData.imsi3DesLookup, $"imsi" === $"number", "left_outer")
      .withColumn("imsi", $"des")
      .drop("des", "number")
      .withColumn("imei", when($"imei".isNotNull, $"imei".substr(0, 8)))
      .withColumn("client_vendor", upper($"client_vendor"))
      .withColumn("client_version", upper($"client_version"))
      .withColumn("terminal_vendor", upper($"terminal_vendor"))
      .withColumn("terminal_model", upper($"terminal_model"))
      .withColumn("terminal_sw_version", upper($"terminal_sw_version"))
      .clientLookup(inputData.client)
      .tacLookup(inputData.tac)
      .terminalLookup(inputData.terminal)
      .withColumn("rcse_terminal_id",
        when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
          .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
            .otherwise($"rcse_terminal_id_desc")
          )
      )
      .drop("rcse_terminal_id_terminal", "rcse_terminal_id_tac", "rcse_terminal_id_desc")
      .terminalSWLookup(inputData.terminalSW)
      .select(
        EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
      )

    logger.info(s"Getting REG,DER-events file, row count: ${nonDM.count()}")

    /*
        nonDM
          .coalesce(1)
          .write
          .option("delimiter", "|")
          .option("header", "false")
          .option("nullValue", "")
          .option("emptyValue", "")
          .option("quoteAll", "false")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv("/Users/ondrejmachacek/tmp/rcse/stage/regDerSpark.csv");


    */
    //Update terminal dimension
    val cols = dimensionBOld.columns.map(i => i + "_old")

    val newTerminal = inputData.terminal
      .drop("entry_id", "load_date")
      .union(dimensionBNew)
      .join(dimensionBOld.toDF(cols: _*), $"rcse_terminal_id" === $"rcse_terminal_id_old", "left_outer")
      .withColumn("tac_code", when($"tac_code".isNull, $"tac_code_old").otherwise($"tac_code"))
      .withColumn("terminal_id", when($"terminal_id".isNull, $"terminal_id_old").otherwise($"terminal_id"))
      .withColumn("rcse_terminal_vendor_sdesc", when($"rcse_terminal_vendor_sdesc".isNull, $"rcse_terminal_vendor_sdesc_old").otherwise($"rcse_terminal_vendor_sdesc"))
      .withColumn("rcse_terminal_vendor_ldesc", when($"rcse_terminal_vendor_ldesc".isNull, $"rcse_terminal_vendor_ldesc_old").otherwise($"rcse_terminal_vendor_ldesc"))
      .withColumn("rcse_terminal_model_sdesc", when($"rcse_terminal_model_sdesc".isNull, $"rcse_terminal_model_sdesc_old").otherwise($"rcse_terminal_model_sdesc"))
      .withColumn("rcse_terminal_model_ldesc", when($"rcse_terminal_model_ldesc".isNull, $"rcse_terminal_model_ldesc_old").otherwise($"rcse_terminal_model_ldesc"))
      .withColumn("modification_date", when($"modification_date".isNull, $"modification_date_old").otherwise($"modification_date"))
      .select(
        "rcse_terminal_id",
        "tac_code",
        "terminal_id",
        "rcse_terminal_vendor_sdesc",
        "rcse_terminal_vendor_ldesc",
        "rcse_terminal_model_sdesc",
        "rcse_terminal_model_ldesc",
        "modification_date"
      )


    logger.info(s"Writing new terminal file, row count: ${newTerminal.count()}")

    val newClient = inputData.client
      .drop("entry_id", "load_date")
      .union(dimensionA)


    newClient.printSchema()
    /*
        newClient
          .coalesce(1)
          .write
          .option("delimiter", "|")
          .option("header", "false")
          .option("nullValue", "")
          .option("emptyValue", "")
          .option("quoteAll", "false")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv("/Users/ondrejmachacek/tmp/rcse/stage/clientSpark.csv");


        newTerminal
          .coalesce(1)
          .write
          .option("delimiter", "|")
          .option("header", "false")
          .option("nullValue", "")
          .option("emptyValue", "")
          .option("quoteAll", "false")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv("/Users/ondrejmachacek/tmp/rcse/stage/terminalSpark.csv");
    */
    //dimension output

    val outputPrep =
      dimensionD
        .withColumn("date_id", $"date_id".cast(DateType))
        //.withColumn("msisdn", when($"msisdn".isNotNull, encoder3des($"msisdn")).otherwise(encoder3des(lit("#"))))
        .join(inputData.msisdn3DesLookup, $"msisdn" === $"number", "left_outer")
        .withColumn("msisdn", $"des")
        .drop("des", "number")
        .withColumnRenamed("rcse_client_id", "rcse_client_id_old")
        .clientLookup(newClient)
        .withColumn("rcse_client_id", when($"rcse_client_id_old".isNull, $"rcse_client_id").otherwise($"rcse_client_id_old"))
        .terminalSimpleLookup(newTerminal)
        .terminalDescLookup(newTerminal)
        .withColumn("rcse_terminal_id",
          when($"rcse_terminal_id".isNotNull, $"rcse_terminal_id")
            .otherwise(
              when($"rcse_terminal_id_terminal".isNotNull, $"rcse_terminal_id_terminal")
                .otherwise(when($"rcse_terminal_id_tac".isNotNull, $"rcse_terminal_id_tac")
                  .otherwise($"rcse_terminal_id_desc")
                )
            )
        )

    val outputDone = outputPrep
      .filter($"rcse_terminal_sw_id".isNotNull)
      .select(
        EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
      )

    val output = outputPrep
      .filter($"rcse_terminal_sw_id".isNull)
      .drop("rcse_terminal_sw_id")
      .terminalSWLookup(inputData.terminalSW.drop("entry_id", "load_date").union(dimensionC))
      .select(
        EventsStage.stageColumns.head, EventsStage.stageColumns.tail: _*
      )
      .union(outputDone)


    logger.info(s"Getting new DM file, row count ${output.count()}")
    output
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", "true")
      .option("nullValue", "")
      .option("emptyValue", "")
      .option("quoteAll", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/Users/ondrejmachacek/tmp/rcse/stage/cptm_ta_f_rcse_events.TMD.20200607.dm.csv");
  }

}
