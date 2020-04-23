package com.tmobile.sit.ignite.hotspot.data

import java.sql.Timestamp

import org.apache.spark.sql.types._

object OrderDBStructures {
  /*
  error_code: Option[String],
                    error_message: Option[String],
                    error_desc: Option[String],
                    valid_from: Option[Timestamp],
                    valid_to: Option[Timestamp],
                    entry_id: Option[Long],
                    load_date: Option[Timestamp]
   */
  val errorCodesStructure = StructType(
    Seq(
      StructField("error_code", StringType, true),
      StructField("error_message", StringType, true),
      StructField("error_desc", StringType, true),
      StructField("valid_from", TimestampType, true),
      StructField("valid_to", TimestampType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  )

  case class OrderDBInput(
                           data_code: Option[String],
                           transaction_id: Option[String],
                           transaction_date: Option[String],
                           transaction_time: Option[String],
                           paytid: Option[String],
                           error_code: Option[String],
                           email: Option[String],
                           amount: Option[Double],
                           currency: Option[String],
                           result_code: Option[String],
                           cancellation: Option[String],
                           card_institute: Option[String],
                           vat: Option[Double],
                           payment_method: Option[String],
                           voucher_type: Option[String],
                           hotspot_name: Option[String],
                           natco: Option[String],
                           username: Option[String],
                           ma_name: Option[String],
                           voucher_duration: Option[Long],
                           number_miles: Option[Double],
                           alternate_currency: Option[String],
                           reduced_amount: Option[Double],
                           campaign_name: Option[String]
                           //  rest: Option[String]
                         )

  case class ErrorCode(
                        error_code: Option[String],
                        error_message: Option[String],
                        error_desc: Option[String],
                        valid_from: Option[Timestamp],
                        valid_to: Option[Timestamp],
                        entry_id: Option[Long],
                        load_date: Option[Timestamp]
                      )



  case class OrderDBOut(
                         wlan_hotspot_id: Option[Int],
                         wlan_hotspot_ident_code: Option[String],
                         wlan_hotspot_desc: Option[String],
                         wlan_hotspot_timezone: Option[String],
                         wlan_venue_type_code: Option[String] = Some("Hotspot not assigned"),
                         wlan_venue_code: Option[String],
                         wlan_provider_code: Option[String],
                         country_code: Option[String],
                         city_code: Option[String],
                         valid_from: Option[Timestamp],
                         valid_to: Option[Timestamp]
                       )
}