package com.tmobile.sit.ignite.hotspot.data

import com.tmobile.sit.ignite.hotspot.processors.{getDouble, getLong, getString}

object CDRStructures {
  case class CDRInput(
                       data_code: Option[String],
                       session_id: Option[String],
                       user_name: Option[String],
                       framed_ip_address: Option[String],
                       session_start_ts: Option[Long],
                       session_duration: Option[Long],
                       session_volume: Option[Long],
                       user_provider_id: Option[String],
                       hotspot_partner_id: Option[String],
                       hotspot_owner_id: Option[String],
                       hotspot_id: Option[String],
                       session_event_ts: Option[Long],
                       terminate_cause_id: Option[Long],
                       wlan_user_account_id: Option[Long],
                       country_code: Option[String],
                       login_type: Option[String],
                       msisdn: Option[String],
                       venue_type: Option[String],
                       venue: Option[String],
                       english_city_name: Option[String]
                     )
  object CDRInput {
    def apply(line: String) : CDRInput = {
      val arr = line.split(";",-1)
      new CDRInput(
        data_code=Some(arr(0)),
        session_id=Some(arr(1)),
        user_name=Some(arr(2)),
        framed_ip_address=Some(arr(3)),
        session_start_ts=getLong(arr(4)),
        session_duration=getLong(arr(5)),
        session_volume=getLong(arr(6)),
        user_provider_id=Some(arr(7)),
        hotspot_partner_id=Some(arr(8)),
        hotspot_owner_id=Some(arr(9)),
        hotspot_id=Some(arr(10)),
        session_event_ts=getLong(arr(11)),
        terminate_cause_id=getLong(arr(12)),
        wlan_user_account_id=getLong(arr(13)),
        country_code=Some(arr(14)),
        login_type=Some(arr(15)),
        msisdn=if (arr(16).length > 0) Some(arr(16)) else None,
        venue_type=Some(arr(17)),
        venue=Some(arr(18)),
        english_city_name=Some(arr(19))
      )
    }
  }
}
