package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

case class InputData(activity: DataFrame, provision: DataFrame, register_requests: DataFrame)

case class PreprocessedData( activity:DataFrame,provision: DataFrame, registerRequests: DataFrame)

case class OutputData(AccActivity:DataFrame,AccProvision:DataFrame,AccRegisterRequests:DataFrame,UserAgents: DataFrame,
                      ProvisionedDaily: DataFrame,ProvisionedMonthly:DataFrame,ProvisionedYearly:DataFrame,
                      RegisteredDaily: DataFrame,RegisteredMonthly: DataFrame,RegisteredYearly: DataFrame,
                      ActiveDaily: DataFrame,ActiveMonthly: DataFrame,ActiveYearly: DataFrame,
                      ServiceDaily:DataFrame)

case class PersistentData(oldUserAgents: DataFrame, accumulated_activity: DataFrame,accumulated_provision:DataFrame,accumulated_register_requests:DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)