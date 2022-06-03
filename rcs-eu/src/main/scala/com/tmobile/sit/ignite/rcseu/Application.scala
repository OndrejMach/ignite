package com.tmobile.sit.ignite.rcseu

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.rcseu.config.{RunConfig, Settings}
import com.tmobile.sit.ignite.rcseu.data.{InputDataProvider, PersistentDataProvider}
import com.tmobile.sit.ignite.rcseu.pipeline._


object RunMode extends Enumeration {
  type RunMode = Value

  val YEARLY, DAILY, UPDATE, EOY = Value
}

object Application extends App with Logger {
  require(args.length == 3, "Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk> <runFor:yearly|daily|update>")

  // Get the run variables based on input arguments
  val settings: Settings = Settings.loadFile(Settings.getConfigFile())
  val runConfig = RunConfig.fromArgs(args)

  logger.info(s"Date: ${runConfig.date}, month:${runConfig.month}, year:${runConfig.year}, natco:${runConfig.natco}, " +
      s"natcoNetwork: ${runConfig.natcoNetwork}, runMode:${runConfig.runMode} ")

  // Get settings and create spark session
  implicit val sparkSession = getSparkSession(settings.appName.get)
  val inputDataProvider = new InputDataProvider(settings, runConfig)
  val persitentDataProvider = new PersistentDataProvider(settings, runConfig)

  logger.info("Input files loaded")

  val archiveActivity = Stage.preprocessAccumulator(persitentDataProvider.getActivityArchives())
  val archiveProvision = Stage.preprocessAccumulator(persitentDataProvider.getProvisionArchives())
  val archiveRegisterRequests = Stage.preprocessAccumulator(persitentDataProvider.getRegisterRequestArchives())

  val accActivity = Stage.accumulateActivity(inputDataProvider.getActivityDf(), archiveActivity, runConfig)
  val accProvision = Stage.accumulateProvision(inputDataProvider.getProvisionFiles(), archiveProvision, runConfig)
  val accRegisterRequests = Stage.accumulateRegisterRequests(inputDataProvider.getRegisterRequests(),
    archiveRegisterRequests, runConfig)

  val newUserAgents = DimensionProcessing.getNewUserAgents(inputDataProvider.getActivityDf(), inputDataProvider.getRegisterRequests())
  val fullUserAgents = DimensionProcessing.processUserAgentsSCD(persitentDataProvider.getOldUserAgents(), newUserAgents)

  val processing = new ProcessingCore(runConfig)

  val outputPath = settings.outputPath.get
  val resultWriter = new ResultWriter(settings, outputPath)

  // yearly processing only
  runConfig.runMode match {
    case RunMode.YEARLY => {
      val (activeYearly, provisionedYearly, registeredYearly) =
        processing.launchYearlyProcessing(accActivity, accProvision, accRegisterRequests, fullUserAgents)

      val fileSuffix = runConfig.natco + "." + runConfig.year + ".csv"

      resultWriter.writeWithFixedEmptyDFs(activeYearly, "activity_yearly." + fileSuffix)
      resultWriter.writeWithFixedEmptyDFs(provisionedYearly, "provisioned_yearly." + fileSuffix)
      resultWriter.writeWithFixedEmptyDFs(registeredYearly, "registered_yearly." + fileSuffix)
    }
    case RunMode.UPDATE => {
      val (activeDaily, activeMonthly, serviceDaily) = processing.processDailyUpdate(accActivity, fullUserAgents)
      resultWriter.writeWithFixedEmptyDFs(activeDaily, "activity_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(activeMonthly, "activity_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(serviceDaily, "service_fact." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
    }
    case RunMode.DAILY => {
      val (activeDaily, activeMonthly, serviceDaily) = processing.processDailyUpdate(accActivity, fullUserAgents)
      val (provisionedDaily, registeredDaily, provisionedMonthly, registeredMonthly) =
        processing.processFullDaily(accProvision, accRegisterRequests, fullUserAgents)

      resultWriter.writeWithFixedEmptyDFs(activeDaily, "activity_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(activeMonthly, "activity_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(serviceDaily, "service_fact." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")

      resultWriter.writeWithFixedEmptyDFs(provisionedDaily, "provisioned_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(registeredDaily, "registered_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(provisionedMonthly, "provisioned_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(registeredMonthly, "registered_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv")
    }
    case RunMode.EOY => {
      val (activeDaily, activeMonthly, serviceDaily) = processing.processDailyUpdate(accActivity, fullUserAgents)
      val activeYearly = processing.endOfYearProcessing(accActivity, fullUserAgents)

      resultWriter.writeWithFixedEmptyDFs(activeDaily, "activity_daily." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(activeMonthly, "activity_monthly." + runConfig.natco + "." + runConfig.monthforoutput + ".csv")
      resultWriter.writeWithFixedEmptyDFs(serviceDaily, "service_fact." + runConfig.natco + "." + runConfig.dateforoutput + ".csv")

      resultWriter.writeWithFixedEmptyDFs(activeYearly, "activity_yearly." + runConfig.natco + "." + runConfig.year + ".csv")
    }
  }

  val persistencePath = settings.lookupPath.get
  CSVWriter(fullUserAgents, persistencePath + "User_agents.csv", delimiter = "\t").writeData()
}
