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
  def preprocessActivity(activity: DataFrame) : DataFrame
  def preprocessProvision(input: DataFrame) : DataFrame
  def preprocessRegisterRequests(input: DataFrame) : DataFrame
}


class Stage extends StageProcessing {
  override def preprocessActivity(activity: DataFrame): DataFrame = {

    //TODO: add logic, similar to RBM
    logger.info("Preprocessing Activity Accumulator")
    val dailyFile = activity
      .withColumn("FileDate", regexp_extract(input_file_name, ".*/activity(.*)_.*csv.gz", 1))
      //.select("FileDate", "creation_date",	"from_user",	"to_user",	"from_network",	"to_network",	"type",	"call_id",	"sip_code",	"user_agent",	"messages_sent",	"messages_received",	"from_tenant",	"to_tenant")
        .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

  /* val dailyFile1= accumulated_activity
     .withColumn("FileDate",lit(null))
     .drop("bytes_sent","bytes_received","contribution_id","duration","src_ip","sip_reason")

      .filter(col("FileDate") =!= "2020-02-28")
      //.withColumn("Date", col("Date").cast("date"))
      //.withColumn("FileDate", col("FileDate").cast("date"))
      //.select("FileDate",  "Date", "NatCo", "user_id")
      .union(dailyFile)
      .orderBy("FileDate")*/

    dailyFile
  }



  override def preprocessProvision(input: DataFrame): DataFrame = {
    input
  }

  override def preprocessRegisterRequests(input: DataFrame): DataFrame = {
    input
  }
}
