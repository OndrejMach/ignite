package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.config.OutputFiles
import org.apache.spark.sql.{DataFrame, SparkSession}

class FullOutputWriter(outputConf: OutputFiles, output: FullOutputs )(implicit sparkSession: SparkSession) extends InflightWriter(outputConf.timestampFormat.get) {

  override def writeOutput(): Unit = {
    logger.info("Full output data ready for writing")
    writeOut(outputConf.path.get + outputConf.flightLegFile.get, output.flightLeg)
    writeOut(outputConf.path.get + outputConf.airportFile.get, output.airport)
    writeOut(outputConf.path.get + outputConf.aircraftFile.get, output.aircraft)
    writeOut(outputConf.path.get + outputConf.airlineFile.get, output.airline)
    writeOut(outputConf.path.get + outputConf.oooiFile.get, output.oooi)
    writeOut(outputConf.path.get + outputConf.radiusFile.get, output.radius)
  }
}
