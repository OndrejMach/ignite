package com.tmobile.sit.ignite.inflight.processing.writers

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure
import com.tmobile.sit.ignite.inflight.processing.{FullOutputs, TransformDataFrameColumns}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * A wrapper class holding writes for the full files output
 * @param outputConf - configuration for the output files
 * @param output - output data
 * @param sparkSession - always here
 */
class FullOutputWriter(outputConf: OutputFiles, output: FullOutputs )(implicit sparkSession: SparkSession) extends InflightWriterUTF8Char(outputConf.timestampFormat.get) {

  override def writeOutput(): Unit = {
    import TransformDataFrameColumns.TransformColumnNames
    logger.info("Full output data ready for writing")

    Seq("wlif_flight_id", "wlif_flightleg_status",
      "wlif_airline_code", "wlif_aircraft_code",
      "wlif_flight_number", "wlif_airport_code_origin", "" +
        "wlif_airport_code_destination",
      "wlif_date_time_opened", "wlif_num_users",
      "wlif_num_sessions", "wlif_session_time",
      "wlif_session_volume_out", "wlif_session_volume_in")


    writeData(outputConf.path.get + outputConf.flightLegFile.get,
      output.flightLeg.select(OutputStructure.flightLeg.head, OutputStructure.flightLeg.tail : _*)
      .columnsToUpperCase()//.na.fill(Timestamp.valueOf("0000-00-00 00:00:00"), Seq("wlif_date_time_opened"))
    )
    writeData(outputConf.path.get + outputConf.airportFile.get, output.airport.select(OutputStructure.airport.head, OutputStructure.airport.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.aircraftFile.get, output.aircraft.select(OutputStructure.aircraft.head, OutputStructure.aircraft.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.airlineFile.get, output.airline.select(OutputStructure.airline.head, OutputStructure.airline.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.oooiFile.get, output.oooi.select(OutputStructure.oooi.head, OutputStructure.oooi.tail : _*).columnsToUpperCase())
    writeData(outputConf.path.get + outputConf.radiusFile.get, output.radius.select(OutputStructure.radius.head, OutputStructure.radius.tail : _*).columnsToUpperCase())
  }
}
