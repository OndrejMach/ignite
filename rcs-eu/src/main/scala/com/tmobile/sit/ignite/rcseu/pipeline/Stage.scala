package com.tmobile.sit.ignite.rcseu.pipeline

import breeze.linalg.split
import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.Application.date
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.{DataFrame, SparkSession}


trait StageProcessing extends Logger{
  def preprocessActivity(activity: DataFrame,accumulated_activity:DataFrame) : DataFrame
  def preprocessProvision(provision: DataFrame, accumulated_provision:DataFrame) : DataFrame
  def preprocessRegisterRequests(register_requests: DataFrame, accumulated_register_requests:DataFrame) : DataFrame
}


class Stage extends StageProcessing {
  override def preprocessActivity(activity: DataFrame,accumulated_activity:DataFrame): DataFrame = {

    //TODO: add logic, similar to RBM
    logger.info("Preprocessing Activity Accumulator")
    val dailyFile = activity
      .withColumn("FileDate", lit(date))
        .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

   val dailyFile1= accumulated_activity
     .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      //.withColumn("Date", col("Date").cast("date"))
      //.withColumn("FileDate", col("FileDate").cast("date"))
      //.select("FileDate",  "Date", "NatCo", "user_id")
      .union(dailyFile)
      .orderBy("FileDate")

    dailyFile1
  }



  override def preprocessProvision(provision: DataFrame, accumulated_provision:DataFrame): DataFrame = {
    //TODO: add logic, similar to RBM
    logger.info("Preprocessing Provision Accumulator")
    val dailyFileProvision = provision
      .withColumn("FileDate", lit(date))
     // .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    val dailyFileProvision1= accumulated_provision
      //.drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      //.withColumn("Date", col("Date").cast("date"))
      //.withColumn("FileDate", col("FileDate").cast("date"))
      //.select("FileDate",  "Date", "NatCo", "user_id")
      .union(dailyFileProvision)
      .orderBy("FileDate")

    dailyFileProvision1
  }

  override def preprocessRegisterRequests(register_requests: DataFrame,accumulated_register_requests:DataFrame): DataFrame = {
    logger.info("Preprocessing Register Requests Accumulator")
    val dailyFileRegister = register_requests
      .withColumn("FileDate", lit(date))
    // .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

    val dailyFileRegister1= accumulated_register_requests
      //.drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")
      .filter(col("FileDate") =!= date)
      //.withColumn("Date", col("Date").cast("date"))
      //.withColumn("FileDate", col("FileDate").cast("date"))
      //.select("FileDate",  "Date", "NatCo", "user_id")
      .union(dailyFileRegister)
      .orderBy("FileDate")

    dailyFileRegister1
  }
}
