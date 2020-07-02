package com.tmobile.sit.ignite.rcseu.pipeline

import java.awt.Dimension

import com.tmobile.sit.ignite.rcseu.data.{OutputData, PreprocessedData}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

trait ProcessingCore {
  def process(preprocessedData: PreprocessedData) : OutputData
}

class Core extends ProcessingCore {

  override def process(stageData: PreprocessedData): OutputData = {

    stageData.activity.show()
    stageData.provision.show()
    stageData.registerRequests.show()

    val dim = new Dimension()

    val UserAgents = dim.getUserAgents(stageData.activity, stageData.registerRequests)

    OutputData(UserAgents)
  }
}