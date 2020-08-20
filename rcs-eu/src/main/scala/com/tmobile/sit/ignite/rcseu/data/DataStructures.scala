package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

case class InputData(activity: Reader, provision: Reader, register_requests: Reader)
//TODO:HERE!!! activity or activity_accumulator????
case class PreprocessedData( activity:DataFrame,provision: DataFrame, registerRequests: DataFrame)

case class OutputData(AccActivity:DataFrame,UserAgents: DataFrame, ProvisionedDaily: DataFrame,RegisteredDaily: DataFrame,ActiveDaily: DataFrame)

case class PersistentData(oldUserAgents: DataFrame, accumulated_activity: DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)