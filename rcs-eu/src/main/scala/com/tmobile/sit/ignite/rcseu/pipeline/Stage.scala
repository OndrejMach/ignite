package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.functions._
import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.RunConfig
import org.apache.spark.sql.functions.{col, lit, reverse, split}
import org.apache.spark.sql.DataFrame

object Stage extends Logger {

  def accumulateActivity(daily_activity: DataFrame, archive_activity:DataFrame, runConfig: RunConfig): DataFrame = {
    //taking today's file (the file with the date from program argument) and adding it to the accumulator
    //eventually replacing the data from the current day processing
    logger.info("Preprocessing Activity Accumulator")

    val dailyFile = daily_activity
      //.withColumn("FileDate", lit(runVar.date))
      .withColumn("FilePath", input_file_name)
      .withColumn("FileName", reverse(split(col("FilePath"),"\\/")).getItem(0))
      .withColumn("FileDate", split(split(col("FileName"),"\\_").getItem(1),"\\.").getItem(0))
      .drop("FilePath", "FileName")
      //.drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    //dailyFile
     // .filter(col("type") === "FT_POST" && (col("from_network") <=> col("to_network")))
      //  .filter()

    logger.info(s"Daily file count: ${dailyFile.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runConfig.date} and adding daily file")

    var resultTmp = archive_activity
      //.drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= lit(runConfig.date.toString))

    // When doing an update, remove tomorrow's activity date from the accumulator before joining on today's data
    // which may or may not include tomorrow
    if(runConfig.runMode.equals("update")){
      resultTmp = resultTmp.filter(col("FileDate") =!= lit(runConfig.tomorrowDate.toString))
      logger.info(s"Update mode. Filtering out old accumulator data for FileDate ${runConfig.tomorrowDate} and adding daily file")
      }

    val result = resultTmp
      .union(dailyFile)
      .orderBy("FileDate")

    result
  }

  def accumulateProvision(daily_provision: DataFrame, archive_provision:DataFrame, runConfig: RunConfig): DataFrame = {
   logger.info("Preprocessing Provision Accumulator")
    val dailyFileProvision = daily_provision
      .withColumn("FileDate", lit(runConfig.date.toString()))
      .select("msisdn", "FileDate")

    logger.info(s"Daily file count: ${dailyFileProvision.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runConfig.date} and adding daily file")

    val dailyFileProvision1= archive_provision
      .filter(col("FileDate") =!= lit(runConfig.date.toString()))
      .select("msisdn", "FileDate")
      //.withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFileProvision)
      .orderBy("FileDate")
    //logger.info(s"New provision accumulator count: ${dailyFileProvision1.count()}")

    dailyFileProvision1
  }

  def accumulateRegisterRequests(daily_register_requests: DataFrame, archive_register_requests:DataFrame,
                                          runConfig: RunConfig): DataFrame = {
    logger.info("Preprocessing Register Requests Accumulator")
    val dailyFileRegister = daily_register_requests
      .withColumn("FileDate", lit(runConfig.date.toString))
      .select("msisdn", "user_agent", "FileDate")

    logger.info(s"Daily file count: ${dailyFileRegister.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runConfig.date} and adding daily file")

    val dailyFileRegister1= archive_register_requests
      .filter(col("FileDate") =!= lit(runConfig.date.toString))
      .select("msisdn", "user_agent", "FileDate")
      //.withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFileRegister)
      .orderBy("FileDate")
    //logger.info(s"New register requests count: ${dailyFileRegister1.count()}")

    dailyFileRegister1
  }

  def preprocessAccumulator(archive: DataFrame):DataFrame = {

      val int = archive
        .withColumn("FilePath", input_file_name())
    //int.show(false)

      //int.withColumn("reverse",reverse(split(col("FilePath"),"\\/") )).select("FilePath", "reverse").show(false)

       int
        .withColumn("FilePath", regexp_replace(col("FilePath"),"register_requests", "registerrequests"))
        .withColumn("FileName", reverse(split(col("FilePath"),"\\/")).getItem(0))
        .withColumn("FileDate", split(split(col("FileName"),"\\_").getItem(1),"\\.").getItem(0))
        .drop("FilePath", "FileName")
        // .withColumn("FileDate", when(col("FileDate").isNull,date_trunc("yyyy-MM-dd",col("creation_date"))).otherwise(col("FileDate")))
  }
}
