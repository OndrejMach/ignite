package com.tmobile.sit.ignite.inflight.processing.writers

import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure
import com.tmobile.sit.ignite.inflight.processing.{FullOutputs, TransformDataFrameColumns}
import org.apache.spark.sql.SparkSession

class FullOutputWriter(outputConf: OutputFiles, output: FullOutputs )(implicit sparkSession: SparkSession) extends InflightWriterUTF8Char(outputConf.timestampFormat.get) {

  override def writeOutput(): Unit = {
    import TransformDataFrameColumns.TransformColumnNames
    logger.info("Full output data ready for writing")
    writeData(outputConf.path.get + outputConf.flightLegFile.get, output.flightLeg.select(OutputStructure.flightLeg.head, OutputStructure.flightLeg.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.airportFile.get, output.airport.select(OutputStructure.airport.head, OutputStructure.airport.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.aircraftFile.get, output.aircraft.select(OutputStructure.aircraft.head, OutputStructure.aircraft.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.airlineFile.get, output.airline.select(OutputStructure.airline.head, OutputStructure.airline.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.oooiFile.get, output.oooi.select(OutputStructure.oooi.head, OutputStructure.oooi.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.radiusFile.get, output.radius.select(OutputStructure.radius.head, OutputStructure.radius.tail : _*).columnsToUpperCase())
  }
}
