package com.tmobile.sit.ignite.rcseu.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.rcseu.config.RunConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class ProcessingCore(runConfig: RunConfig) extends Logger {
  val factsProcessor = new FactsProcessing(runConfig)

  def launchYearlyProcessing( accumulatedActivity: DataFrame,
                              accumulatedProvisions: DataFrame,
                              accumulatedRegisterRequests: DataFrame,
                              fullUserAgents: DataFrame
                            ): Tuple3[DataFrame, DataFrame, DataFrame] = {
    logger.info("Processing yearly activity")
    val accActivityYearlyFiltered =
      accumulatedActivity.filter(col("creation_date").contains(runConfig.year))


    val activeYearly1 = factsProcessor.getActiveDaily(
      accActivityYearlyFiltered,
      fullUserAgents,
      runConfig.year
    )

    val activeYearly = activeYearly1
      .withColumnRenamed("ConKeyA1", "ConKeyA3")
      .withColumnRenamed(
        "Active_daily_succ_origterm",
        "Active_yearly_succ_origterm"
      )
      .withColumnRenamed("Active_daily_succ_orig", "Active_yearly_succ_orig")
      .withColumnRenamed(
        "Active_daily_unsucc_origterm",
        "Active_yearly_unsucc_origterm"
      )
      .withColumnRenamed(
        "Active_daily_unsucc_orig",
        "Active_yearly_unsucc_orig"
      )
    //logger.info("Active yearly count: " + activeYearly.count())

    //**********************************************Provision***********************************************//

    logger.info("Processing yearly provisioned")
    //TODO: check with git:
    //    val filtered_yearly_provision = acc_provision//.filter(col("FileDate").contains(runVar.year))
    val provisionedYearly1 = factsProcessor.getProvisionedDaily(
      accumulatedProvisions,
      runConfig.year
    )

    val provisionedYearly = provisionedYearly1
      .withColumnRenamed("ConKeyP1", "ConKeyP3")
      .withColumnRenamed("Provisioned_daily", "Provisioned_yearly")
    //logger.info("Provisioned yearly count: " + provisionedYearly.count())

    //*******************************************Register Requests******************************************//

    //    if(runVar.processYearly) {
    logger.info("Processing yearly register requests")
    // TODO: check with git:
    //    val filtered_yearly_register = acc_register_requests//.filter(col("FileDate").contains(runVar.year))
    val registeredYearly1 = factsProcessor.getRegisteredDaily(
      accumulatedRegisterRequests,
      fullUserAgents,
      runConfig.year
    )
    val registeredYearly = registeredYearly1
      .withColumnRenamed("ConKeyR1", "ConKeyR3")
      .withColumnRenamed("Registered_daily", "Registered_yearly")

    (activeYearly, provisionedYearly, registeredYearly)
  }

  def processDailyUpdate(accumulatedActivity: DataFrame,
                          fullUserAgents: DataFrame
                        ): Tuple3[DataFrame, DataFrame, DataFrame] = {
    logger.info("Processing daily activity") // here we need the accumulator
    val accActivityDailyFiltered = accumulatedActivity.filter(
      col("creation_date").contains(runConfig.date.toString)
    )

    val activeDaily = factsProcessor.getActiveDaily(
      accActivityDailyFiltered,
      fullUserAgents,
      runConfig.dayforkey
    )
    //logger.info("Active daily count: " + activeDaily.count())

    logger.info("Processing monthly activity")
    val accActivityMonthlyFiltered =
      accumulatedActivity.filter(col("creation_date").contains(runConfig.month))
    val activeMonthly1 = factsProcessor.getActiveDaily(
      accActivityMonthlyFiltered,
      fullUserAgents,
      runConfig.monthforkey
    )

    val activeMonthly = activeMonthly1
      .withColumnRenamed("ConKeyA1", "ConKeyA2")
      .withColumnRenamed(
        "Active_daily_succ_origterm",
        "Active_monthly_succ_origterm"
      )
      .withColumnRenamed("Active_daily_succ_orig", "Active_monthly_succ_orig")
      .withColumnRenamed(
        "Active_daily_unsucc_origterm",
        "Active_monthly_unsucc_origterm"
      )
      .withColumnRenamed(
        "Active_daily_unsucc_orig",
        "Active_monthly_unsucc_orig"
      )
    //logger.info("Active monthly count: " + activeMonthly.count())

    //*********************************************Service Fact*********************************************//

    logger.info("Processing daily service fact")
    val serviceDaily =
      factsProcessor.getServiceFactsDaily(accumulatedActivity)

    (activeDaily, activeMonthly, serviceDaily)
  }

  def processFullDaily(accumulatedProvision: DataFrame, accumulatedRegisterRequests: DataFrame,
                       fullUserAgents: DataFrame
                      ): Tuple4[DataFrame, DataFrame, DataFrame, DataFrame] = {
    //**********************************************Provision***********************************************//
    // TODO: can we optimize this to read only the daily file?
    logger.info("Processing daily provisioned")
    val accProvisionFilteredDaily = accumulatedProvision.filter(col("FileDate") === runConfig.date.toString)
    //val filtered_daily_provision = inputData.provision
    val provisionedDaily = factsProcessor.getProvisionedDaily(accProvisionFilteredDaily, runConfig.dayforkey)
    //logger.info("Provisioned daily count: " + provisionedDaily.count())

    logger.info("Processing monthly provisioned")
    val accProvisionFilteredMonthly = accumulatedProvision.filter(col("FileDate").contains(runConfig.month))
    val provisionedMonthly1 = factsProcessor.getProvisionedDaily(accProvisionFilteredMonthly, runConfig.monthforkey)
    val provisionedMonthly = provisionedMonthly1
      .withColumnRenamed("ConKeyP1", "ConKeyP2")
      .withColumnRenamed("Provisioned_daily", "Provisioned_monthly")
    //logger.info("Provisioned monthly count: " + provisionedMonthly.count())

    //*******************************************Register Requests******************************************//

    // TODO: can we optimize this to read only the daily file?
    logger.info("Processing daily register requests")
    val filtered_daily_register = accumulatedRegisterRequests.filter(col("FileDate") === runConfig.date.toString)
    //val filtered_daily_register = inputData.register_requests
    val registeredDaily = factsProcessor.getRegisteredDaily(filtered_daily_register, fullUserAgents, runConfig.dayforkey)
    //logger.info("Registered daily count: " + registeredDaily.count())

    logger.info(s"Processing monthly register requests")
    val filtered_monthly_register = accumulatedRegisterRequests.filter(col("FileDate").contains(runConfig.month))
    val registeredMonthly1 = factsProcessor.getRegisteredDaily(filtered_monthly_register, fullUserAgents, runConfig.monthforkey)
    val registeredMonthly =
      registeredMonthly1
        .withColumnRenamed("ConKeyR1", "ConKeyR2")
        .withColumnRenamed("Registered_daily", "Registered_monthly")
    //logger.info("Registered monthly count: " + registeredMonthly.count())

    (provisionedDaily, registeredDaily, provisionedMonthly, registeredMonthly)
  }

  def endOfYearProcessing(accumulatedActivity: DataFrame, fullUserAgents: DataFrame): DataFrame = {
    //*********************************************Yearly Activity******************************************//
    logger.info("Running special case for end-of-year update. Overwriting yearly activity data.")
    logger.info("Processing yearly activity")
    val filtered_yearly_active = accumulatedActivity.filter(col("creation_date").contains(runConfig.year))
    val activeYearly1 = factsProcessor.getActiveDaily(filtered_yearly_active, fullUserAgents, runConfig.year)

    val activeYearly = activeYearly1.withColumnRenamed("ConKeyA1", "ConKeyA3")
      .withColumnRenamed("Active_daily_succ_origterm", "Active_yearly_succ_origterm")
      .withColumnRenamed("Active_daily_succ_orig", "Active_yearly_succ_orig")
      .withColumnRenamed("Active_daily_unsucc_origterm", "Active_yearly_unsucc_origterm")
      .withColumnRenamed("Active_daily_unsucc_orig", "Active_yearly_unsucc_orig")

    activeYearly
  }
}
