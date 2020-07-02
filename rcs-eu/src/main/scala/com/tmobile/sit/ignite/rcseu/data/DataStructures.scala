package com.tmobile.sit.ignite.rcseu.data

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

case class InputData(activity: Reader, provision: Reader, register_requests: Reader)

case class PreprocessedData(activity: DataFrame, provision: DataFrame, registerRequests: DataFrame)

case class OutputData(UserAgents: DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)