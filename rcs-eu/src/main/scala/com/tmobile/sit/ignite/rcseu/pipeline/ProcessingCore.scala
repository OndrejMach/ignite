package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.data.{OutputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.functions.monotonically_increasing_id


trait ProcessingCore extends Logger{
  def process(preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

class Core extends ProcessingCore {

  override def process(stageData: PreprocessedData, persistentData: PersistentData): OutputData = {

    //stageData.activity.show()
    //stageData.provision.show()
    //stageData.registerRequests.show()

    val stage = new Stage()

    val acc_activity = stage.preprocessActivity(stageData.activity)
    logger.info("Activity accumulator count: " + acc_activity.count())




    val dim = new Dimension()

    // logic for UserAgents dimension
    val newUserAgents = dim.getNewUserAgents(stageData.activity, stageData.registerRequests)

    val fullUserAgents0 = dim.processUserAgentsSCD(persistentData.oldUserAgents, newUserAgents).dropDuplicates("UserAgent")
    val fullUserAgents =fullUserAgents0.withColumn("_UserAgentID", monotonically_increasing_id)
    fullUserAgents.cache()
    logger.info("Full user agents count: " + fullUserAgents.count())

    // Process facts
    val fact = new Facts()
    val provisionedDaily = fact.getProvisionedDaily(stageData.provision)
    logger.info("Provisioned daily count: " + provisionedDaily.count())

    val registeredDaily = fact.getRegisteredDaily(stageData.registerRequests,fullUserAgents)
    logger.info("Registered daily count: " + registeredDaily.count())

    val activeDaily = fact.getActiveDaily(stageData.activity,fullUserAgents)
    logger.info("Active daily count: " + activeDaily.count())

    val serviceDaily = fact.getServiceFactsDaily(stageData.activity)
    logger.info("Service facts daily count: " + activeDaily.count())


    OutputData(acc_activity,fullUserAgents,provisionedDaily,registeredDaily,activeDaily,serviceDaily)
    //TODO: add also here writer
  }
}