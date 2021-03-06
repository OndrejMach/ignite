package com.tmobile.sit.ignite.rcseu.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class InputData(activity: DataFrame, provision: DataFrame, register_requests: DataFrame)

case class PersistentData(oldUserAgents: DataFrame,
                          activity_archives: DataFrame,
                          provision_archives:DataFrame,
                          register_requests_archives:DataFrame)

case class PreprocessedData(acc_activity:DataFrame, acc_provision: DataFrame, acc_register_requests: DataFrame)

case class OutputData(AccActivity:DataFrame,AccProvision:DataFrame,AccRegisterRequests:DataFrame,UserAgents: DataFrame,
                      ProvisionedDaily: DataFrame,ProvisionedMonthly:DataFrame,ProvisionedYearly:DataFrame,
                      RegisteredDaily: DataFrame,RegisteredMonthly: DataFrame,RegisteredYearly: DataFrame,
                      ActiveDaily: DataFrame,ActiveMonthly: DataFrame,ActiveYearly: DataFrame,
                      ServiceDaily:DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)

object FileSchemas {
  val activitySchema = StructType(
    Seq(
      StructField("creation_date"      ,StringType , false),
      StructField("from_user"          ,StringType , false),
      StructField("to_user"            ,StringType , false),
      StructField("from_network"       ,StringType , false),
      StructField("to_network"         ,StringType , false),
      StructField("type"               ,StringType , false),
      StructField("duration"           ,IntegerType , true),
      StructField("bytes_sent"         ,IntegerType , true),
      StructField("bytes_received"     ,IntegerType , true),
      StructField("call_id"            ,StringType , false),
      StructField("contribution_id"    ,StringType , false),
      StructField("src_ip"             ,StringType , false),
      StructField("sip_code"           ,IntegerType , true),
      StructField("sip_reason"         ,StringType , false),
      StructField("user_agent"         ,StringType , false),
      StructField("messages_sent"      ,IntegerType , true),
      StructField("messages_received"  ,IntegerType , true),
      StructField("from_tenant"        ,StringType , false),
      StructField("to_tenant"          ,StringType , false)
    )
  )

  val provisionSchema = StructType(
    Seq(
      StructField("msisdn"      ,StringType , false),
      StructField("tenant"      ,StringType , false)
    )
  )
  val registerRequestsSchema = StructType(
    Seq(
      StructField("msisdn"      ,StringType , false),
      StructField("user_agent"  ,StringType , false),
      StructField("tenant"      ,StringType , false)
    )
  )

  val accActivitySchema = StructType(
    Seq(
      StructField("creation_date"      ,StringType , false),
      StructField("from_user"          ,StringType , false),
      StructField("to_user"            ,StringType , false),
      StructField("from_network"       ,StringType , false),
      StructField("to_network"         ,StringType , false),
      StructField("type"               ,StringType , false),
      StructField("call_id"            ,StringType , false),
      StructField("sip_code"           ,IntegerType , true),
      StructField("user_agent"         ,StringType , false),
      StructField("messages_sent"      ,IntegerType , true),
      StructField("messages_received"  ,IntegerType , true),
      StructField("from_tenant"        ,StringType , false),
      StructField("to_tenant"          ,StringType , false),
      StructField("FileDate"           ,StringType , false)
    )
  )

  val accProvidionSchema = StructType(
    Seq(
      StructField("msisdn"      ,StringType , false),
      StructField("FileDate"    ,StringType , false)
    )
  )

  val accRegisterRequestsSchema = StructType(
    Seq(
      StructField("msisdn"      ,StringType , false),
      StructField("user_agent"  ,StringType , false),
      StructField("FileDate"    ,StringType , false)
    )
  )
}