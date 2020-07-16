package com.tmobile.sit.ignite.rcseu.pipeline

//import breeze.linalg.split
import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait FactsProcessing extends Logger{
  def getProvisionedDaily(provisionData: DataFrame): DataFrame
}

class Facts extends FactsProcessing {

  def getProvisionedDaily(provision: DataFrame): DataFrame = {
    //TODO: add logic here to aggregate provisioned users
    val provisionedDaily = provision
      .withColumn("ConKeyP1", regexp_extract(input_file_name, ".*/provision_(.*)_.*csv.gz", 1))
      .withColumn("NatCo", regexp_extract(input_file_name, ".*/provision_.*_(.*).csv.gz", 1))
      .withColumn("ConKeyP1", concat_ws("|",col("ConKeyP1"),col("NatCo")))
      .groupBy("ConKeyP1").count().withColumnRenamed("count","Provisioned_daily")
    provisionedDaily
  }

  def getRegisteredDaily(register_requests: DataFrame): DataFrame = {
    //TODO: add logic here to aggregate registered users
    val dfRMT1=register_requests.withColumn("ConKeyR1", regexp_extract(input_file_name, ".*/register_requests_(.*).csv.gz", 1))
      .groupBy("msisdn")
      .agg(max("user_agent").alias("user_agent"),max("ConKeyR1").alias("ConKeyR1"))
      .withColumn("ConKeyR1",regexp_replace(col("ConKeyR1"), "_", "|"))
      .withColumn("ConKeyR1", concat_ws("|",col("ConKeyR1"),col("user_agent")))

    val dfRMT2=register_requests
      .groupBy("msisdn").count()

    val joinedDS = dfRMT1.join(dfRMT2, "msisdn")
      .select("ConKeyR1","count" )
      .withColumnRenamed("count", "Registered_Daily")
    joinedDS
  }

}