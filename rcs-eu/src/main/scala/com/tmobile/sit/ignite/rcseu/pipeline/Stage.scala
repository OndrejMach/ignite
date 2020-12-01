package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.functions._
import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.date
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.DataFrame


trait StageProcessing extends Logger{
  def preprocessActivity(activity:DataFrame,accumulated_activity:DataFrame) : DataFrame
  def preprocessProvision(provision: DataFrame, accumulated_provision:DataFrame) : DataFrame
  def preprocessRegisterRequests(register_requests: DataFrame, accumulated_register_requests:DataFrame) : DataFrame
}

//Creating accumulators for activity data, provision data and register requests data
class Stage extends StageProcessing {

  // TODO: Decide if we need the natco in these
  override def preprocessActivity(activity: DataFrame,accumulated_activity:DataFrame): DataFrame = {
    //taking today's file (the file with the date from program argument) and adding it to the accumulator
    //eventually replacing the data from the current day processing
    //dropping unused columns
    logger.info("Preprocessing Activity Accumulator")

    // Before changing the input schema to take the creation_date as a string, it was
    // a timestamp and was causing issues. The fix was:
    // .withColumn("creation_date", col("creation_date") - expr("INTERVAL 1 HOURS"))
    val dailyFile = activity
      .withColumn("FileDate", lit(date))
      .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    /*
    logger.info("Daily activity source file data:")
    dailyFile
      .withColumn("creation_date", substring(col("creation_date"), 0, 10))
      .groupBy("FileDate", "creation_date")
      .agg(count("creation_date").as("creation_date_count"))
      .orderBy(asc("FileDate"))
      .show(2)

   logger.info("Old accumulator data")
   accumulated_activity
      .groupBy("FileDate")
      .agg(count("creation_date").as("creation_date_count"))
      .orderBy(asc("FileDate"))
      .show(5)
    */

    logger.info(s"Daily file count: ${dailyFile.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${date} and adding daily file")

    val result = accumulated_activity
      .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      .union(dailyFile)
      .orderBy("FileDate")

    //logger.info(s"New activity accumulator: ${result.count()}"+" records")

    result
  }

  override def preprocessProvision(provision: DataFrame, accumulated_provision:DataFrame): DataFrame = {
   logger.info("Preprocessing Provision Accumulator")
    val dailyFileProvision = provision
      .withColumn("FileDate", lit(date))
      .select("msisdn", "FileDate")

    logger.info(s"Daily file count: ${dailyFileProvision.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${date} and adding daily file")

    val dailyFileProvision1= accumulated_provision
      .filter(col("FileDate") =!= date)
      .select("msisdn", "FileDate")
      .withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFileProvision)
      .orderBy("FileDate")
    //logger.info(s"New provision accumulator count: ${dailyFileProvision1.count()}")

    dailyFileProvision1
  }

  override def preprocessRegisterRequests(register_requests: DataFrame,accumulated_register_requests:DataFrame): DataFrame = {
    logger.info("Preprocessing Register Requests Accumulator")
    val dailyFileRegister = register_requests
      .withColumn("FileDate", lit(date))
      .select("msisdn", "user_agent", "FileDate")

    logger.info(s"Daily file count: ${dailyFileRegister.count()}")
    logger.info(s"Filtering out old accumulator data for FileDate ${date} and adding daily file")

    val dailyFileRegister1= accumulated_register_requests
      .filter(col("FileDate") =!= date)
      .select("msisdn", "user_agent", "FileDate")
      .withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFileRegister)
      .orderBy("FileDate")
    //logger.info(s"New register requests count: ${dailyFileRegister1.count()}")

    dailyFileRegister1
  }
}
