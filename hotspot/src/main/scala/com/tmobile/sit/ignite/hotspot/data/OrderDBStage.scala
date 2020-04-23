package com.tmobile.sit.ignite.hotspot.data

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures.OrderDBInput

case class OrderDBStage(
                         ta_id: Option[String],
                         ta_request_date: Option[Date],
                         ta_request_datetime: Option[Timestamp],
                         ta_request_hour: Option[String],
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
                         hotspot_country_code: Option[String],
                         hotspot_provider_code: Option[String],
                         hotspot_venue_type_code: Option[String],
                         hotspot_venue_code: Option[String],
                         hotspot_city_code: Option[String],
                         hotspot_ident_code: Option[String],
                         hotspot_timezone: Option[String],
                         natco: Option[String],
                         username: Option[String],
                         wlan_realm_code: Option[String],
                         ma_name: Option[String],
                         voucher_duration: Option[Long],
                         alternate_amount: Option[Double],
                         alternate_currency: Option[String],
                         reduced_amount: Option[Double],
                         campaign_name: Option[String],
                         entry_id: Option[Long],
                         load_date: Option[Timestamp]
                       )
object OrderDBStage {

  def apply(entry: OrderDBInput): OrderDBStage = {
    val hotspot_vec = entry.hotspot_name.get.split("/", -1)

    def transformString(i: Int, arrayElementNotExist: String, f: (String) => String): Option[String] = {
      if (hotspot_vec.size > i && !hotspot_vec(i).isEmpty) {
        if (hotspot_vec(i) == "*") Some("") else Some(f(hotspot_vec(i)))
      } else Some(arrayElementNotExist)

    }

    val hotspot_country_code = transformString(0, "NN", (s: String) => s.trim.toUpperCase())
    val hotspot_provider_code = transformString(1, "UNDEFINED", (s: String) => s.trim.toUpperCase())
    val hotspot_venue_type_code = transformString(2, "UNDEFINED", (s: String) => s.trim.toUpperCase())
    val hotspot_venue_code = transformString(3, "UNDEFINED", (s: String) => s.trim.toUpperCase())
    val hotspot_timezone = if (hotspot_vec.size > 6 && !hotspot_vec(6).isEmpty) Some(hotspot_vec(6)) else Some("UNDEFINED")
    val hotspot_ident_code = {
      if (hotspot_vec.size > 5 && !hotspot_vec(5).isEmpty) {
        if (hotspot_vec(5) == "*" || hotspot_vec(5).startsWith("DEFAULT") || hotspot_vec(5).startsWith("000000000000")) {
          val underscore_provider = if (hotspot_provider_code.get.isEmpty) "" else "_"
          val underscore_country = if (hotspot_country_code.get.isEmpty) "" else "_"
          Some("undefined_OTHER_OTHER" + underscore_provider + hotspot_provider_code.get + underscore_country + hotspot_country_code.get)
        } else Some(hotspot_vec(5))
      } else Some("UNDEFINED")
    }
    val hotspot_city_code: Option[String] = {
      if (hotspot_vec.size > 4 && !hotspot_vec(4).isEmpty) {
        if (hotspot_vec(4) == "*") {
          Some("Undefined")
        } else {
          Some((for {i <- hotspot_vec.drop(4)} yield i).reduce(_ + "/" + _))
        }
      } else {
        Some("UNDEFINED")
      }
    }

    val ta_td = {
      if (hotspot_provider_code == "TMUK") {
        Timestamp.valueOf(LocalDateTime
          .parse(entry.transaction_date.get + entry.transaction_time.get, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
          .minusHours(1))
      } else {
        Timestamp.valueOf(LocalDateTime
          .parse(entry.transaction_date.get + entry.transaction_time.get, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
      }
    }

    new OrderDBStage(
      result_code = entry.result_code,
      reduced_amount = entry.reduced_amount,
      hotspot_country_code = hotspot_country_code,
      hotspot_provider_code = hotspot_provider_code,
      hotspot_venue_type_code = hotspot_venue_type_code,
      hotspot_venue_code = hotspot_venue_code,
      hotspot_timezone = hotspot_timezone,
      hotspot_ident_code = hotspot_ident_code,
      hotspot_city_code = hotspot_city_code,
      ta_id = entry.transaction_id,
      ta_request_date = Some(Date.valueOf(LocalDate.parse(entry.transaction_date.get, DateTimeFormatter.ofPattern("yyyyMMdd")))),
      ta_request_datetime = Some(ta_td),
      ta_request_hour = Some(ta_td.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHH"))),
      paytid = entry.paytid,
      error_code = entry.error_code,
      email = if (entry.email.isDefined && !entry.email.get.isEmpty) Option(entry.email.get.trim) else Some("#"),
      amount = entry.amount,
      currency = entry.currency,
      cancellation = entry.cancellation,
      card_institute = Some(entry.card_institute.get.toUpperCase()),
      vat = entry.vat,
      payment_method = entry.payment_method,
      voucher_type = entry.voucher_type,
      natco = entry.natco,
      username = if (entry.username.isDefined) Some(entry.username.get.trim) else Some("#"),
      wlan_realm_code = if (entry.username.isDefined) Some(entry.username.get.trim) else Some("#"),
      ma_name = if (entry.ma_name.isDefined) Some(entry.ma_name.get.trim) else Some("#"),
      voucher_duration = entry.voucher_duration,
      alternate_amount = entry.number_miles,
      alternate_currency = entry.alternate_currency,
      campaign_name = entry.campaign_name,
      entry_id = Some(0),
      load_date = Some(Timestamp.valueOf(LocalDateTime.now()))
    )
  }
}