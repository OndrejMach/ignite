package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.data.{OutputData, PersistentData, PreprocessedData}


trait ProcessingCore extends Logger{
  def process(preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

class Core extends ProcessingCore {

  override def process(stageData: PreprocessedData, persistentData: PersistentData): OutputData = {

    //stageData.activity.show()
    //stageData.provision.show()
    //stageData.registerRequests.show()

    val dim = new Dimension()

    // logic for UserAgents dimension
    val newUserAgents = dim.getNewUserAgents(stageData.activity, stageData.registerRequests)
    val fullUserAgents = dim.processUserAgentsSCD(persistentData.oldUserAgents, newUserAgents)
    fullUserAgents.cache()
    logger.info("Full user agents count: " + fullUserAgents.count())

    // Process facts
    val fact = new Facts()
    val provisionedDaily = fact.getProvisionedDaily(stageData.provision)
    logger.info("Provisioned daily count: " + provisionedDaily.count())

    val registeredDaily = fact.getRegisteredDaily(stageData.registerRequests)
    logger.info("Registered daily count: " + registeredDaily.count())
    OutputData(fullUserAgents,provisionedDaily,registeredDaily)
  }
}