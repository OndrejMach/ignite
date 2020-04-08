package com.tmobile.sit.ignite.inflight.processing

import com.tmobile.sit.common.Logger
import com.tmobile.sit.ignite.inflight.processing.data.{OutputFilters, StageData, StagedDataForFullOutput}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.tmobile.sit.ignite.inflight.datastructures.OutputStructure

case class FullOutputs(radius: DataFrame, airport: DataFrame, aircraft: DataFrame, airline: DataFrame, oooi: DataFrame, flightLeg:DataFrame)

/**
 * Implicit class for transforming column names to upper case - used in all the outputs
 */
object TransformDataFrameColumns {
  implicit class TransformColumnNames(df : DataFrame) {
    def columnsToUpperCase() : DataFrame = {
      df.toDF(df.columns.map(_.toUpperCase()):_*)
    }
  }
}

/**
 * Processor class doing daily files calculations (AIRPORT, AIRLINE, FLIGHLEG, RADIUS, AIRCRAFT, OOOI, REALM)
 * @param stagedInput - raw files preprocessed
 * @param airlines - airline codes relevant for the output
 * @param sparkSession - hu
 */
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
      radius = data.radius.select(OutputStructure.radius.head, OutputStructure.radius.tail : _*).columnsToUpperCase(),
      airport = data.airport.select(OutputStructure.airport.head, OutputStructure.airport.tail :_*).columnsToUpperCase(),
      aircraft = data.aircraft.select(OutputStructure.aircraft.head, OutputStructure.aircraft.tail :_*).columnsToUpperCase(),
      airline = data.airline.select(OutputStructure.airline.head, OutputStructure.airline.tail :_*).columnsToUpperCase(),
      oooi = data.oooi.select(OutputStructure.oooi.head, OutputStructure.oooi.tail :_*).columnsToUpperCase(),
      flightLeg = data.flightLeg.select(OutputStructure.flightLeg.head, OutputStructure.flightLeg.tail :_*).columnsToUpperCase()
    )
  }


  def generateOutput() =  {
    logger.info("Preparing data for full-files output")
    val filteredData = filterStagedData(stagedInput)
    projectOutputColumns(filteredData)
  }
}
