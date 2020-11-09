package com.tmobile.sit.ignite.exchangerates.processing

import com.tmobile.sit.ignite.common.data.CommonStructures

/**
 * output columns for stage files.
 */

object StageStructures {

  val EXCHANGE_RATES_OUTPUT_COLUMNS : Seq[String]= CommonStructures.exchangeRatesStructure.map(_.name)

}
