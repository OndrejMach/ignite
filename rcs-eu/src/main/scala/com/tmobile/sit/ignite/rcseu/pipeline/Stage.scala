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
  def preprocessProvision(input: DataFrame) : DataFrame
  def preprocessRegisterRequests(input: DataFrame) : DataFrame
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



  override def preprocessProvision(input: DataFrame): DataFrame = {
    input
  }

  override def preprocessRegisterRequests(input: DataFrame): DataFrame = {
    input
  }
}
