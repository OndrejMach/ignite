package com.tmobile.sit.ignite.common.data

import java.sql.Timestamp
import java.sql.Date

object CommonTypes {
  case class ExchangeRates(
                            currency_code: Option[String],
                            exchange_rate_code: Option[String],
                            exchange_rate_avg: Option[Double],
                            exchange_rate_sell: Option[Double],
                            exchange_rate_buy: Option[Double],
                            faktv: Option[Long],
                            faktn: Option[Long],
                            period_from: Option[Timestamp],
                            period_to: Option[Timestamp],
                            valid_from: Option[Date],
                            valid_to: Option[Date]
                            //entry_id: Option[Long],
                            //load_date: Option[Timestamp]
                          )

}
