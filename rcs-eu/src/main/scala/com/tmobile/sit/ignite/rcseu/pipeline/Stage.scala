package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.functions._
import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.date
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType


trait StageProcessing extends Logger{
  def preprocessActivity(activity:DataFrame,accumulated_activity:DataFrame) : DataFrame
  def preprocessProvision(provision: DataFrame, accumulated_provision:DataFrame) : DataFrame
  def preprocessRegisterRequests(register_requests: DataFrame, accumulated_register_requests:DataFrame) : DataFrame
}

//Creating accumulators for activity data, provision data and register requests data
class Stage extends StageProcessing {
  override def preprocessActivity(activity: DataFrame,accumulated_activity:DataFrame): DataFrame = {
    //taking today's file (the file with the date from program argument) and adding it to the accumulator
    //eventually replacing the data from the current day processing
    //dropping unused columns
    logger.info("Preprocessing Activity Accumulator")

    //TODO: remove this
    //logger.info("Daily Activity file")
    //activity.printSchema()

    //Fixing creation date by substracting one hour from the timestamp
    val dailyFile = activity
      .withColumn("FileDate", lit(date))
      //.withColumn("creation_date", col("creation_date") - expr("INTERVAL 1 HOURS"))
      //.withColumn("creation_date", substring(col("creation_date_fixed"), 0, 10))
      .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    // TODO: remove this debug

    logger.info("Daily file schema")
    dailyFile.printSchema()

    logger.info("Daily file data")
    dailyFile
      .withColumn("creation_date", substring(col("creation_date"), 0, 10))
      .groupBy("FileDate", "creation_date")
      .agg(count("creation_date").as("creation_date_count"))
      .orderBy(asc("FileDate"))
      .show(5)


    // TODO: remove this debug

    logger.info("Old accumulator data")
    accumulated_activity
      .groupBy("FileDate")
      .agg(count("creation_date").as("creation_date_count"))
      .orderBy(asc("FileDate"))
      .show(5)


    logger.info(s"accumulated_activity ${accumulated_activity.filter(col("FileDate") =!= date).count()} daily file: ${dailyFile.count()}")

    logger.info(s"Filtering out old accumulator data for date ${date} and adding daily file")

    val result = accumulated_activity
      .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      //.withColumn("creation_date", substring(col("creation_date"), 0, 10))
      //.withColumn("FileDate", col("FileDate").cast("date"))
      .union(dailyFile)
      .orderBy("FileDate")

    logger.info(s"result count: ${result.count()}")

    // TODO: remove this debug

    logger.info("New accumulator data")
    result
      .groupBy("FileDate")
      .agg(count("creation_date").as("creation_date_count"))
      .orderBy(asc("FileDate"))
      .show(5)


    result
  }



  override def preprocessProvision(provision: DataFrame, accumulated_provision:DataFrame): DataFrame = {
    //TODO: add logic, similar to RBM
    logger.info("Preprocessing Provision Accumulator")
    val dailyFileProvision = provision
      .withColumn("FileDate", lit(date))
     // .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    logger.info(s"accumulated_provision ${accumulated_provision.filter(col("FileDate") =!= date).count()} daily file: ${dailyFileProvision.count()}")
    val dailyFileProvision1= accumulated_provision
      //.drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      //.withColumn("Date", col("Date").cast("date"))
      .withColumn("FileDate", col("FileDate").cast("date"))
      //.select("FileDate",  "Date", "NatCo", "user_id")
      .union(dailyFileProvision)
      .orderBy("FileDate")
    logger.info(s"result count: ${dailyFileProvision1.count()}")

    dailyFileProvision1
  }

  override def preprocessRegisterRequests(register_requests: DataFrame,accumulated_register_requests:DataFrame): DataFrame = {
    logger.info("Preprocessing Register Requests Accumulator")
    val dailyFileRegister = register_requests
      .withColumn("FileDate", lit(date))
    // .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    logger.info(s"accumulated_register ${accumulated_register_requests.filter(col("FileDate") =!= date).count()} daily file: ${dailyFileRegister.count()}")
    val dailyFileRegister1= accumulated_register_requests
      //.drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      //.withColumn("Date", col("Date").cast("date"))
      .withColumn("FileDate", col("FileDate").cast("date"))
      //.select("FileDate",  "Date", "NatCo", "user_id")
      .union(dailyFileRegister)
      .orderBy("FileDate")
    logger.info(s"result count: ${dailyFileRegister1.count()}")

    dailyFileRegister1
  }
}
