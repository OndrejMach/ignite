package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.processing.data.{OutputColumns, OutputFilters, StagedInput}
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed case class Outputs(radius: DataFrame, airport: DataFrame, aircraft: DataFrame, airline: DataFrame, oooi: DataFrame, flightLeg:DataFrame)

object TransformDataFrameColumns {
  implicit class TransformColumnNames(df : DataFrame) {
    def columnsToUpperCase() : DataFrame = {
      df.toDF(df.columns.map(_.toUpperCase()):_*)
    }
  }
}


class FullOutputsProcessor(stagedInput: StagedInput, outputPath: String)(implicit sparkSession: SparkSession) extends OutputWriter {
  val flightLegFilename = outputPath + "CPTM_QV_F_WLIF_FLIGHTLEG.csv"
  val airportFilename = outputPath + "CPTM_TA_D_WLIF_AIRPORT.csv"
  val aircraftFilename = outputPath + "CPTM_QV_D_WLIF_AIRCRAFT.csv"
  val airlineFilename = outputPath + "CPTM_QV_D_WLIF_AIRLINE.csv"
  val oooiFilename = outputPath + "CPTM_QV_F_WLIF_OOOI.csv"
  val radiusFilename = outputPath + "CPTM_QV_F_WLIF_RADIUS.csv"



  private def filterStagedData(stagedData: StagedInput) : StagedInput = {
    StagedInput(
      radius = stagedData.radius.filter(OutputFilters.filterAirline()),
      airport = stagedData.airport,
      aircraft = stagedData.aircraft,
      airline = stagedData.airline.filter(OutputFilters.filterAirline()),
      oooi = stagedData.oooi.filter(OutputFilters.filterAirline()),
      flightLeg = stagedData.flightLeg.filter(OutputFilters.filterAirline()),
      realm = stagedData.realm
    )
  }

  private def projectOutputColumns(data: StagedInput): Outputs = {
    import TransformDataFrameColumns.TransformColumnNames
    Outputs(
      radius = data.radius.select(OutputColumns.radius.head, OutputColumns.radius.tail : _*).columnsToUpperCase(),
      airport = data.airport.select(OutputColumns.airport.head, OutputColumns.airport.tail :_*).columnsToUpperCase(),
      aircraft = data.aircraft.select(OutputColumns.aircraft.head, OutputColumns.aircraft.tail :_*).columnsToUpperCase(),
      airline = data.airline.select(OutputColumns.airline.head, OutputColumns.airline.tail :_*).columnsToUpperCase(),
      oooi = data.oooi.select(OutputColumns.oooi.head, OutputColumns.oooi.tail :_*).columnsToUpperCase(),
      flightLeg = data.flightLeg.select(OutputColumns.flightLeg.head, OutputColumns.flightLeg.tail :_*).columnsToUpperCase()
    )
  }

  private def writeOut(path: String, data: DataFrame) = {
    val writer = CSVWriter(data, path, delimiter = "|", timestampFormat = "yyyy-MM-dd HH:mm:ss")
    writer.writeData()
  }

  def writeOutput() =  {
    val filteredData = filterStagedData(stagedInput)
    val output = projectOutputColumns(filteredData)

    writeOut(flightLegFilename, output.flightLeg)
    writeOut(airportFilename, output.airport)
    writeOut(aircraftFilename, output.aircraft)
    writeOut(airlineFilename, output.airline)
    writeOut(oooiFilename, output.oooi)
    writeOut(radiusFilename, output.radius)
  }
}
