package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.functions._
import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.runVar
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.DataFrame

trait StageProcessing extends Logger{
  def accumulateActivity(activity:DataFrame, accumulated_activity:DataFrame) : DataFrame
  def accumulateProvision(provision: DataFrame, accumulated_provision:DataFrame) : DataFrame
  def accumulateRegisterRequests(register_requests: DataFrame, accumulated_register_requests:DataFrame) : DataFrame
  def preprocessAccumulator(archive: DataFrame):DataFrame
}

class Stage extends StageProcessing {

  override def accumulateActivity(activity: DataFrame, accumulated_activity:DataFrame): DataFrame = {
    //taking today's file (the file with the date from program argument) and adding it to the accumulator
    //eventually replacing the data from the current day processing
    //dropping unused columns
    logger.info("Preprocessing Activity Accumulator")

    // Before changing the input schema to take the creation_date as a string, it was
    // a timestamp and was causing issues. The fix was:
    // .withColumn("creation_date", col("creation_date") - expr("INTERVAL 1 HOURS"))
    val dailyFile = activity
      .withColumn("FileDate", lit(runVar.date))
      .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    logger.info(s"Daily file count: ${dailyFile.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runVar.date} and adding daily file")

    val result = accumulated_activity
      .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= runVar.date)
      .union(dailyFile)
      .orderBy("FileDate")

    result
  }

  override def accumulateProvision(provision: DataFrame, accumulated_provision:DataFrame): DataFrame = {
   logger.info("Preprocessing Provision Accumulator")
    val dailyFileProvision = provision
      .withColumn("FileDate", lit(runVar.date))
      .select("msisdn", "FileDate")

    logger.info(s"Daily file count: ${dailyFileProvision.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runVar.date} and adding daily file")

    val dailyFileProvision1= accumulated_provision
      .filter(col("FileDate") =!= runVar.date)
      .select("msisdn", "FileDate")
      //.withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFileProvision)
      .orderBy("FileDate")
    //logger.info(s"New provision accumulator count: ${dailyFileProvision1.count()}")

    dailyFileProvision1
  }

  override def accumulateRegisterRequests(register_requests: DataFrame, accumulated_register_requests:DataFrame): DataFrame = {
    logger.info("Preprocessing Register Requests Accumulator")
    val dailyFileRegister = register_requests
      .withColumn("FileDate", lit(runVar.date))
      .select("msisdn", "user_agent", "FileDate")

    logger.info(s"Daily file count: ${dailyFileRegister.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${runVar.date} and adding daily file")

    val dailyFileRegister1= accumulated_register_requests
      .filter(col("FileDate") =!= runVar.date)
      .select("msisdn", "user_agent", "FileDate")
      //.withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFileRegister)
      .orderBy("FileDate")
    //logger.info(s"New register requests count: ${dailyFileRegister1.count()}")

    dailyFileRegister1
  }

  override def preprocessAccumulator(archive: DataFrame):DataFrame = {

      archive
        .withColumn("FilePath", input_file_name)
        .withColumn("FilePath", regexp_replace(col("FilePath"),"register_requests", "registerrequests"))
        .withColumn("FileName", reverse(split(col("filePath"),"\\/")).getItem(0))
        .withColumn("FileDate", split(split(col("fileName"),"\\_").getItem(1),"\\.").getItem(0))
        .drop("FilePath", "FileName")
  }
}
