package com.tmobile.sit.ignite.inflight.processing

import java.sql.Timestamp

import com.tmobile.sit.ignite.inflight.processing.aggregates.{AggregateRadiusCredit, AggregateRadiusCreditData}
import com.tmobile.sit.ignite.inflight.processing.data.{InputData, ReferenceData, StageData}
import com.tmobile.sit.ignite.inflight.translateSeconds
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class RadiusCreditDailyProcessor(refData: ReferenceData, stageData: StageData,
                                 firstDate: Timestamp, lastPlus1Date: Timestamp, minRequestDate: Timestamp
                                 )(implicit sparkSession: SparkSession, runId: Int, loadDate: Timestamp) extends Processor {

  def executeProcessing(): DataFrame = {

    import TransformDataFrameColumns.TransformColumnNames
    val aggregateRadiusCredit = new AggregateRadiusCreditData(radius = stageData.radius, voucher = refData.voucher, orderDB = refData.orderDB, exchangeRates = refData.exchangeRates,
      firstDate = firstDate, lastPlus1Date = lastPlus1Date, minRequestDate = minRequestDate)


    val processing = new AggregateRadiusCredit(aggregateRadiusCredit)

    val result = processing
      .executeProcessing()

     // result.printSchema()
      result.select("wlif_session_stop","wlif_aircraft_code",
      "wlif_flight_id","wlif_airline_code",
      "wlif_flight_number","wlif_airport_code_origin",
      "wlif_airport_code_destination","wlif_username",
      "wlif_realm_code","wlan_hotspot_ident_code",
      "payid","amount_incl_vat","amount_excl_vat",
      "card_institute","payment_method",
      "voucher_type","voucher_duration",
      "wlif_num_sessions","wlif_session_volume","wlif_session_time")
      .columnsToUpperCase()

  }
}
