package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.functions._
import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.runVar
import org.apache.spark.sql.functions.{col, lit, split, reverse}
import org.apache.spark.sql.DataFrame

trait StageProcessing extends Logger{
  def accumulateActivity(daily_activity:DataFrame, archive_activity:DataFrame) : DataFrame
  def accumulateProvision(daily_provision: DataFrame, archive_provision:DataFrame) : DataFrame
  def accumulateRegisterRequests(daily_register_requests: DataFrame, archive_register_requests:DataFrame) : DataFrame
  def preprocessAccumulator(archive: DataFrame):DataFrame
}

class Stage extends StageProcessing {

  override def accumulateActivity(daily_activity: DataFrame, archive_activity:DataFrame): DataFrame = {
    //taking today's file (the file with the date from program argument) and adding it to the accumulator
    //eventually replacing the data from the current day processing
    logger.info("Preprocessing Activity Accumulator")

    val dailyFile = daily_activity

    logger.info(s"Daily file count: ${dailyFile.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runVar.date} and adding daily file")

    var resultTmp = archive_activity
      .filter(col("FileDate") =!= runVar.date)

    // When doing an update, remove tomorrow's activity date from the accumulator before joining on today's data
    // which may or may not include tomorrow
    if(runVar.runMode.equals("update")){
      resultTmp = resultTmp.filter(col("FileDate") =!= runVar.tomorrowDate)
      logger.info(s"Update mode. Filtering out old accumulator data for FileDate ${runVar.tomorrowDate} and adding daily file")
      }

    val result = resultTmp
      .union(dailyFile)
      .orderBy("FileDate")
    result
  }

  override def accumulateProvision(daily_provision: DataFrame, archive_provision:DataFrame): DataFrame = {
   logger.info("Preprocessing Provision Accumulator")
    val dailyFileProvision = daily_provision
//      .withColumn("FileDate", lit(runVar.date))
      .select("msisdn", "FileDate")

    logger.info(s"Daily file count: ${dailyFileProvision.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runVar.date} and adding daily file")

    val dailyFileProvision1= archive_provision
      .filter(col("FileDate") =!= runVar.date)
      .select("msisdn", "FileDate")
      .union(dailyFileProvision)
      .orderBy("FileDate")
    dailyFileProvision1
  }

  override def accumulateRegisterRequests(daily_register_requests: DataFrame, archive_register_requests:DataFrame): DataFrame = {
    logger.info("Preprocessing Register Requests Accumulator")
    val dailyFileRegister = daily_register_requests
      .select("msisdn", "user_agent", "FileDate")

    logger.info(s"Daily file count: ${dailyFileRegister.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runVar.date} and adding daily file")

    val dailyFileRegister1= archive_register_requests
      .filter(col("FileDate") =!= runVar.date)
      .select("msisdn", "user_agent", "FileDate")
      .union(dailyFileRegister)
      .orderBy("FileDate")

    dailyFileRegister1
  }

  override def preprocessAccumulator(archive: DataFrame):DataFrame = {

      val int = archive
        .withColumn("FilePath", input_file_name())

       int
        .withColumn("FilePath", regexp_replace(col("FilePath"),"register_requests", "registerrequests"))
        .withColumn("FileName", reverse(split(col("FilePath"),"\\/")).getItem(0))
        .withColumn("FileDate", split(split(col("FileName"),"\\_").getItem(1),"\\.").getItem(0))
        .drop("FilePath", "FileName")
  }
}
