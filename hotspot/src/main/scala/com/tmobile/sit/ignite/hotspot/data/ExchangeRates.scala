package com.tmobile.sit.ignite.hotspot.data

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.tmobile.sit.common.Logger

case class ExchangeRates(row_id: Option[String],
                         ratetype: Option[String],
                         fromcurrency: Option[String],
                         tocurrency: Option[String],
                         valid_from: Option[Date],
                         exchange_rate: Option[Double],
                         multiplierfromcurrency: Option[Int],
                         multipliertocurrency: Option[Int]
                        )

object ExchangeRates extends Logger {
 def fromString(line: String): ExchangeRates = {
    val arr = line.split("\\|")
    logger.debug(s"Retrieved exchange rates line ${line} split to ${arr}")
    ExchangeRates(row_id = Some(arr(0)),
      ratetype = Some(arr(1)),
      fromcurrency = Some(arr(2)),
      tocurrency = Some(arr(3)),
      valid_from = try {
        Some(Date.valueOf(LocalDate.parse(arr(4), DateTimeFormatter.ofPattern("yyyyMMdd"))))
      } catch {
        case e: Exception => None
      },
      exchange_rate = try {
        Some(arr(5).toDouble)
      } catch {
        case e: Exception => None
      },
      multiplierfromcurrency = try {
        Some(arr(6).toInt)
      } catch {
        case e: Exception => None
      },
      multipliertocurrency = try {
        Some(arr(7).toInt)
      } catch {
        case e: Exception => None
      }
    )
  }
}