package com.tmobile.sit.ignite.rcseutoparquet

import com.tmobile.sit.ignite.common.common.Logger
import com.tmobile.sit.ignite.rcseu.config.RunConfig
import com.tmobile.sit.ignite.rcseu.pipeline.{Configurator, Helper}
import com.tmobile.sit.ignite.rcseutoparquet.data.CSVToParquet
import org.glassfish.jersey.internal.inject.ReferenceTransformingFactory.Transformer

object Application extends App with Logger{
  if (args.length != 3) {
    logger.error("Wrong arguments. Usage: ... <date:yyyy-mm-dd> <natco:mt|cg|st|cr|mk> <runFor:yearly|daily|update>")
    System.exit(0)
  }

  val runVar = new RunConfig(args)

  val settings = new Configurator().getSettings()
  implicit val sparkSession = getSparkSession(settings.appName.get)

  // Instantiate helper and resolve source file paths
  val h = new Helper()
  val inputFilePath = h.resolveInputPath(settings)
  val sourceFilePath = h.resolvePath(settings)

  val transformer = new CSVToParquet()

  transformer.csvToParquet(inputFilePath = inputFilePath, sourceFilePath = sourceFilePath)

}
