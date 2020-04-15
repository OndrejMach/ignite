package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.ignite.common.data.CommonStructures

object OutputStructures {
  val EXCHANGE_RATES_OUTPUT_COLUMNS : Seq[String]= CommonStructures.exchangeRatesStructure.map(_.name)

  //Seq("currency_code", "exchange_rate_code", "exchange_rate_avg", "exchange_rate_sell", "exchange_rate_buy", "faktv","faktn","period_from", "period_to", "valid_from","valid_to", "entry_id", "load_date")
}
