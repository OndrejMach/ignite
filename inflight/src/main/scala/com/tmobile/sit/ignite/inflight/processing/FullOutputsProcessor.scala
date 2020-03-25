package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.processing.data.{OutputColumns, OutputFilters, StageData, StagedDataForFullOutput}
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed case class Outputs(radius: DataFrame, airport: DataFrame, aircraft: DataFrame, airline: DataFrame, oooi: DataFrame, flightLeg:DataFrame)

object TransformDataFrameColumns {
  implicit class TransformColumnNames(df : DataFrame) {
    def columnsToUpperCase() : DataFrame = {
      df.toDF(df.columns.map(_.toUpperCase()):_*)
    }
  }
}


class FullOutputsProcessor(stagedInput: StageData, outputConf: OutputFiles, airlines: Seq[String])(implicit sparkSession: SparkSession) extends OutputWriter with Logger {


  private def filterStagedData(stagedData: StageData) : StagedDataForFullOutput = {
    logger.info(s"Filtering output data for full output files (airlines: ${airlines.mkString(",")})")
    StagedDataForFullOutput(
      radius = stagedData.radius.filter(OutputFilters.filterAirline(airlines)),
      airport = stagedData.airport,
      aircraft = stagedData.aircraft,
      airline = stagedData.airline.filter(OutputFilters.filterAirline(airlines)),
      oooi = stagedData.oooi.filter(OutputFilters.filterAirline(airlines)),
      flightLeg = stagedData.flightLeg.filter(OutputFilters.filterAirline(airlines)),
      realm = stagedData.realm
    )
  }

  private def projectOutputColumns(data: StagedDataForFullOutput): Outputs = {
    import TransformDataFrameColumns.TransformColumnNames
    logger.info("Preparing output structures for full files output")
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
    logger.info(s"Writing output file ${path}")
    val writer = CSVWriter(data, path, delimiter = "|", timestampFormat = outputConf.timestampFormat.get)
    writer.writeData()
  }

  def writeOutput() =  {
    logger.info("Preparing data for full-files output")
    val filteredData = filterStagedData(stagedInput)
    val output = projectOutputColumns(filteredData)
    logger.info("Data ready for writing")
    writeOut(outputConf.path.get + outputConf.flightLegFile.get, output.flightLeg)
    writeOut(outputConf.path.get + outputConf.airportFile.get, output.airport)
    writeOut(outputConf.path.get + outputConf.aircraftFile.get, output.aircraft)
    writeOut(outputConf.path.get + outputConf.airlineFile.get, output.airline)
    writeOut(outputConf.path.get + outputConf.oooiFile.get, output.oooi)
    writeOut(outputConf.path.get + outputConf.radiusFile.get, output.radius)
  }
}
