package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

case class InputData(activity: Reader, provision: Reader, register_requests: Reader)

case class PreprocessedData(activityData: DataFrame, provisionData: DataFrame, registerRequestsData: DataFrame)
