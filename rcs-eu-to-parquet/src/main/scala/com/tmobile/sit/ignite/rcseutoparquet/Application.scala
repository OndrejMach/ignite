package com.tmobile.sit.ignite.rcseutoparquet

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcseutoparquet.config.RunConfig
import com.tmobile.sit.ignite.rcseu.pipeline.Configurator
import com.tmobile.sit.ignite.rcseutoparquet.data.RCSEUCSVToParquet

object Application extends App with Logger{
  if (args.length != 2) {
    logger.error("Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk>")
    System.exit(0)
  }

  val runVar = new RunConfig(args)

  val settings = new Configurator().getSettings()
  implicit val sparkSession = getSparkSession(settings.appName.get)

  RCSEUCSVToParquet.csvToParquet(inputFilePath = settings.inputPath.get, sourceFilePath = settings.archivePath.get)

}
