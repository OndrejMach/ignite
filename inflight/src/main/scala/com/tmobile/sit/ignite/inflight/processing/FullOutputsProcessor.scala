package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.config.OutputFiles
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


class FullOutputsProcessor(stagedInput: StagedInput, outputConf: OutputFiles, airlines: Seq[String])(implicit sparkSession: SparkSession) extends OutputWriter {


  private def filterStagedData(stagedData: StagedInput) : StagedInput = {
    StagedInput(
      radius = stagedData.radius.filter(OutputFilters.filterAirline(airlines)),
      airport = stagedData.airport,
      aircraft = stagedData.aircraft,
      airline = stagedData.airline.filter(OutputFilters.filterAirline(airlines)),
      oooi = stagedData.oooi.filter(OutputFilters.filterAirline(airlines)),
      flightLeg = stagedData.flightLeg.filter(OutputFilters.filterAirline(airlines)),
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
    val writer = CSVWriter(data, path, delimiter = "|", timestampFormat = outputConf.timestampFormat.get)
    writer.writeData()
  }

  def writeOutput() =  {
    val filteredData = filterStagedData(stagedInput)
    val output = projectOutputColumns(filteredData)

    writeOut(outputConf.flightLegFile.get, output.flightLeg)
    writeOut(outputConf.airportFile.get, output.airport)
    writeOut(outputConf.aircraftFile.get, output.aircraft)
    writeOut(outputConf.airlineFile.get, output.airline)
    writeOut(outputConf.oooiFile.get, output.oooi)
    writeOut(outputConf.radiusFile.get, output.radius)
  }
}
