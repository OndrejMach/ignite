package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.data.{InputData, OutputData, PersistentData, PreprocessedData}
import org.apache.spark.sql.functions.col
import com.tmobile.sit.ignite.rcseu.Application.runVar
import org.apache.spark.sql.DataFrame

trait ProcessingCore extends Logger{
  def process(inputData: InputData, preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

class Core extends ProcessingCore {

  override def process(inputData: InputData,stageData: PreprocessedData, persistentData: PersistentData): OutputData = {

    var provisionedDaily:DataFrame = null
    var provisionedMonthly:DataFrame = null
    var provisionedYearly:DataFrame = null
    var registeredDaily:DataFrame = null
    var registeredMonthly:DataFrame = null
    var registeredYearly:DataFrame = null
    var activeDaily:DataFrame = null
    var activeMonthly:DataFrame = null
    var activeYearly:DataFrame = null
    var serviceDaily:DataFrame = null

    val acc_activity = stageData.acc_activity
    acc_activity.cache()
    logger.info("Activity accumulator count: " + acc_activity.count())

    val acc_provision = stageData.acc_provision
    acc_provision.cache()
    logger.info("Provision accumulator count: " + acc_provision.count())

    val acc_register_requests = stageData.acc_register_requests
    acc_register_requests.cache()
    logger.info("Register requests accumulator count: " + acc_register_requests.count())

    //logic for UserAgents dimension, creating daily new file and replacing old one, adding only new user agents
    val dim = new Dimension()

    //TODO: decide if here the new user agents should only be based on daily input files
    val newUserAgents = dim.getNewUserAgents(stageData.acc_activity, stageData.acc_register_requests)
    val fullUserAgents = dim.processUserAgentsSCD(persistentData.oldUserAgents, newUserAgents)
    fullUserAgents.cache()

    if (!runVar.isHistoric) {

      // Processing facts, aggregating accumulated data by date, month, year
      val fact = new Facts()

      logger.info("Processing daily activity")
      val filtered_daily_active = acc_activity.filter(col("creation_date").contains(runVar.date))
      activeDaily = fact.getActiveDaily(filtered_daily_active, fullUserAgents, runVar.dayforkey, runVar.natcoNetwork)
      //logger.info("Active daily count: " + activeDaily.count())

      logger.info("Processing monthly activity")
      val filtered_monthly_active = acc_activity.filter(col("creation_date").contains(runVar.month))
      val activeMonthly1 = fact.getActiveDaily(filtered_monthly_active, fullUserAgents, runVar.monthforkey, runVar.natcoNetwork)
      activeMonthly= activeMonthly1.withColumnRenamed("ConKeyA1","ConKeyA2")
        .withColumnRenamed("Active_daily_succ_origterm", "Active_monthly_succ_origterm")
        .withColumnRenamed("Active_daily_succ_orig", "Active_monthly_succ_orig")
        .withColumnRenamed("Active_daily_unsucc_origterm", "Active_monthly_unsucc_origterm")
        .withColumnRenamed("Active_daily_unsucc_orig", "Active_monthly_unsucc_orig")
      //logger.info("Active monthly count: " + activeMonthly.count())

      logger.info("Processing yearly activity")
      val filtered_yearly_active = acc_activity.filter(col("creation_date").contains(runVar.year))
      val activeYearly1 = fact.getActiveDaily(filtered_yearly_active, fullUserAgents, runVar.year, runVar.natcoNetwork)
      activeYearly= activeYearly1.withColumnRenamed("ConKeyR1","ConKeyR3")
        .withColumnRenamed("Active_daily_succ_origterm", "Active_yearly_succ_origterm")
        .withColumnRenamed("Active_daily_succ_orig", "Active_yearly_succ_orig")
        .withColumnRenamed("Active_daily_unsucc_origterm", "Active_yearly_unsucc_origterm")
        .withColumnRenamed("Active_daily_unsucc_orig", "Active_yearly_unsucc_orig")
      //logger.info("Active yearly count: " + activeYearly.count())

      //******************************************************************************************************//

      logger.info("Processing daily provisioned")
      val filtered_daily_provision = acc_provision.filter(col("FileDate").contains(runVar.date))
      provisionedDaily = fact.getProvisionedDaily(filtered_daily_provision, runVar.dayforkey)
      //logger.info("Provisioned daily count: " + provisionedDaily.count())

      logger.info("Processing monthly provisioned")
      val filtered_monthly_provision = acc_provision.filter(col("FileDate").contains(runVar.month))
      val provisionedMonthly1 = fact.getProvisionedDaily(filtered_monthly_provision, runVar.monthforkey)
      provisionedMonthly= provisionedMonthly1.withColumnRenamed("ConKeyP1","ConKeyP2")
        .withColumnRenamed("Provisioned_daily","Provisioned_monthly")
      //logger.info("Provisioned monthly count: " + provisionedMonthly.count())

      logger.info("Processing yearly provisioned")
      val filtered_yearly_provision = acc_provision.filter(col("FileDate").contains(runVar.year))
      val provisionedYearly1 = fact.getProvisionedDaily(filtered_yearly_provision, runVar.year)
      provisionedYearly= provisionedYearly1.withColumnRenamed("ConKeyP1","ConKeyP3")
        .withColumnRenamed("Provisioned_daily","Provisioned_yearly")
      //logger.info("Provisioned yearly count: " + provisionedYearly.count())

      //******************************************************************************************************//

      logger.info("Processing daily register requests")
      val filtered_daily_register = acc_register_requests.filter(col("FileDate").contains(runVar.date))
      registeredDaily = fact.getRegisteredDaily(filtered_daily_register, fullUserAgents, runVar.dayforkey)
      //logger.info("Registered daily count: " + registeredDaily.count())

      logger.info("Processing monthly register requests")
      val filtered_monthly_register = acc_register_requests.filter(col("FileDate").contains(runVar.month))
      val registeredMonthly1 = fact.getRegisteredDaily(filtered_monthly_register, fullUserAgents, runVar.monthforkey)
      registeredMonthly= registeredMonthly1.withColumnRenamed("ConKeyR1","ConKeyR2")
        .withColumnRenamed("Registered_daily","Registered_monthly")
      //logger.info("Registered monthly count: " + registeredMonthly.count())

      logger.info("Processing yearly register requests")
      val filtered_yearly_register = acc_register_requests.filter(col("FileDate").contains(runVar.year))
      val registeredYearly1 = fact.getRegisteredDaily(filtered_yearly_register, fullUserAgents, runVar.year)
      registeredYearly= registeredYearly1.withColumnRenamed("ConKeyR1","ConKeyR3")
        .withColumnRenamed("Registered_daily","Registered_yearly")
      //logger.info("Registered yearly count: " + registeredYearly.count())

      //******************************************************************************************************//

      //generating one file each day (only daily processing needed)
      logger.info("Processing service fact")
      serviceDaily = fact.getServiceFactsDaily(acc_activity)
      //logger.info("Service facts daily count: " + serviceDaily.count())
    }

    // Create output data object
    OutputData(acc_activity,acc_provision,acc_register_requests,fullUserAgents,
      provisionedDaily,provisionedMonthly,provisionedYearly,
      registeredDaily,registeredMonthly,registeredYearly,
      activeDaily,activeMonthly,activeYearly,
      serviceDaily)
  }
}