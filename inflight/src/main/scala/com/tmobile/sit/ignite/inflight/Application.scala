package com.tmobile.sit.ignite.inflight

import java.sql.Timestamp
import java.time.LocalDateTime

import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, OutputFilters, StageData, StagedInput}
import com.tmobile.sit.ignite.inflight.processing.{AggregateRadiusCredit, AggregateRadiusCreditData, FullOutputsProcessor, StageProcess}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application {


  private def processInput(stageProcess: StageProcess, runId: Int = 0, loadDate: Timestamp = Timestamp.valueOf(LocalDateTime.now()))(implicit sparkSession: SparkSession): StagedInput = {
    StagedInput(
      aircraft = stageProcess.processAircraft(InputData.aircraft),
      airport = stageProcess.processAirport(InputData.airport),
      airline = stageProcess.processAirline(InputData.airline),
      realm = stageProcess.preprocessRealm(InputData.realm),
      radius = stageProcess.processRadius(InputData.radius),
      flightLeg = stageProcess.processFlightLeg(InputData.flightLeg),
      oooi = stageProcess.processOooid(InputData.oooi)
    )
  }



  private def processRadiusVoucher(stageProcess: StageProcess, firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp)(implicit sparkSession: SparkSession): Unit = {

    //radius.select(max("wlif_session_stop")).show(false)

    val radiusPrep = stageProcess.processRadius(InputData.radius)

    //radiusPrep.summary().select("wlif_session_stop").show(true)

    val aggregatevoucher = new AggregateRadiusCreditData(radius = radiusPrep, voucher = StageData.voucher, orderDB = StageData.orderDB, exchangeRates = StageData.exchangeRates,
      firstDate = firstDate, lastPlus1Date = lastPlus1Date, minRequestDate = minRequestDate)


    val processing = new AggregateRadiusCredit(aggregatevoucher, loadDate = Timestamp.valueOf("2020-03-10 00:00:00"), runId = 123)

    val result = processing.executeProcessing()

    result.coalesce(1).write.csv("/Users/ondrejmachacek/tmp/inflightRadiusVoucher.csv")
  }

  private def processAggregateRadiusCredit()(implicit sparkSession: SparkSession): Unit = {

  }

  def main(args: Array[String]): Unit = {
    implicit val sparkSession = getSparkSession()

    val firstDate = Timestamp.valueOf("2019-02-10 00:00:00")
    val lastPlus1Date = Timestamp.valueOf("2020-02-28 00:00:00")
    val minRequestDate = Timestamp.valueOf("2011-02-11 00:00:00")
    val stage = new StageProcess()

    val input = processInput(stage)

    val fullOutput = new FullOutputsProcessor(input, "/Users/ondrejmachacek/tmp/inflight/")

    fullOutput.writeOutput()
  }

}
