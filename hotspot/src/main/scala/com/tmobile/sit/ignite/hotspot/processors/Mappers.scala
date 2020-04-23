package com.tmobile.sit.ignite.hotspot.processors

import com.tmobile.sit.ignite.hotspot.ApplicationOrderDB.{ENTRY_ID, FUTURE, LOAD_DATE}
import com.tmobile.sit.ignite.hotspot.data.OrderDBStructures.{ErrorCode, OrderDBInput}

object Mappers {
  def mapInputOrderDB(line: String): OrderDBInput = {
    val values = line.split(";", -1)
    OrderDBInput(
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

  def mapErrorCodes(entry: OrderDBInput): ErrorCode = {
    ErrorCode(
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
