package com.tmobile.sit.ignite.hotspot.data

import java.sql.Timestamp
import com.tmobile.sit.ignite.hotspot.processors.{getDouble, getLong, getString}
import org.apache.spark.sql.types._

object OrderDBStructures {
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

  object OrderDBInput {
    def apply(line: String): OrderDBInput = {
      val values = line.split(";", -1)
      new OrderDBInput(
        data_code = getString(values(0)),
        transaction_id = getString(values(1)),
        transaction_date = getString(values(2)),
        transaction_time = getString(values(3)),
        paytid = getString(values(4)),
        error_code = getString(values(5)),
        email = getString(values(6)),
        amount = getDouble(values(7)),
        currency = getString(values(8)),
        result_code = getString(values(9)),
        cancellation = getString(values(10)),
        card_institute = getString(values(11)),
        vat = getDouble(values(12)),
        payment_method = getString(values(13)),
        voucher_type = getString(values(14)),
        hotspot_name = getString(values(15)),
        natco = getString(values(16)),
        username = getString(values(17)),
        ma_name = getString(values(18)),
        voucher_duration = getLong(values(19)),
        number_miles = getDouble(values(20)),
        alternate_currency = getString(values(21)),
        reduced_amount = getDouble(values(22)),
        campaign_name = getString(values(23))
      )
    }
  }

  case class ErrorCode(
                        error_code: Option[String],
                        error_message: Option[String],
                        error_desc: Option[String],
                        valid_from: Option[Timestamp],
                        valid_to: Option[Timestamp],
                        entry_id: Option[Long],
                        load_date: Option[Timestamp]
                      )

  object ErrorCode {
    def apply(entry: OrderDBInput): ErrorCode = {
      new ErrorCode(
        error_code = if (entry.error_code.isDefined) Some(entry.error_code.get.toUpperCase) else Some("0000"),
        error_message = if (!entry.error_code.isDefined) Some("UNKNOWN") else Some(entry.error_code.get.toUpperCase),
        error_desc = Some("UNKNOWN"),
        valid_from = Some(LOAD_DATE),
        valid_to = Some(FUTURE),
        entry_id = Some(ENTRY_ID),
        load_date = Some(LOAD_DATE)
      )
    }
  }

}