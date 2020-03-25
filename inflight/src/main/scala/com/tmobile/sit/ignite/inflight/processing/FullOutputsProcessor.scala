package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.config.OutputFiles
import com.tmobile.sit.ignite.inflight.processing.data.{OutputColumns, OutputFilters, StageData, StagedDataForFullOutput}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FullOutputs(radius: DataFrame, airport: DataFrame, aircraft: DataFrame, airline: DataFrame, oooi: DataFrame, flightLeg:DataFrame)

object TransformDataFrameColumns {
  implicit class TransformColumnNames(df : DataFrame) {
    def columnsToUpperCase() : DataFrame = {
      df.toDF(df.columns.map(_.toUpperCase()):_*)
    }
  }
}


class FullOutputsProcessor(stagedInput: StageData, airlines: Seq[String])(implicit sparkSession: SparkSession) extends Logger {


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

  private def projectOutputColumns(data: StagedDataForFullOutput): FullOutputs = {
    import TransformDataFrameColumns.TransformColumnNames
    logger.info("Preparing output structures for full files output")
    FullOutputs(
      radius = data.radius.select(OutputColumns.radius.head, OutputColumns.radius.tail : _*).columnsToUpperCase(),
      airport = data.airport.select(OutputColumns.airport.head, OutputColumns.airport.tail :_*).columnsToUpperCase(),
      aircraft = data.aircraft.select(OutputColumns.aircraft.head, OutputColumns.aircraft.tail :_*).columnsToUpperCase(),
      airline = data.airline.select(OutputColumns.airline.head, OutputColumns.airline.tail :_*).columnsToUpperCase(),
      oooi = data.oooi.select(OutputColumns.oooi.head, OutputColumns.oooi.tail :_*).columnsToUpperCase(),
      flightLeg = data.flightLeg.select(OutputColumns.flightLeg.head, OutputColumns.flightLeg.tail :_*).columnsToUpperCase()
    )
  }


  def generateOutput() =  {
    logger.info("Preparing data for full-files output")
    val filteredData = filterStagedData(stagedInput)
    projectOutputColumns(filteredData)
  }
}
