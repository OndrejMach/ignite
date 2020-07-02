package com.tmobile.sit.ignite.rcseu.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

trait ProcessingCore {
  def process(preprocessedData: PreprocessedData) : OutputData
}

class CoreLogicWithTransform extends ProcessingCore {

  def getUserAgents(activityData: DataFrame, registerRequestsData: DataFrame): DataFrame = {
    activityData
      .select("user_agent")
      .union(
        registerRequestsData
          .select("user_agent")
      )
      .distinct()
      .sort("user_agent")
  }

  override def process(preprocessedData: PreprocessedData): OutputData = {

    preprocessedData.activityData.show()
    preprocessedData.provisionData.show()
    preprocessedData.registerRequestsData.show()

    val UserAgents = getUserAgents(preprocessedData.activityData, preprocessedData.registerRequestsData)

    OutputData(UserAgents)
  }
}