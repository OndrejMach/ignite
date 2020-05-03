package com.tmobile.sit.ignite.hotspot.data

object FailedLoginsStructure {

  case class FailedLogin(
                          data_code: Option[String],
                          hotspot_provider_code: Option[String],
                          hotspot_venue_code: Option[String],
                          hotspot_venue_type_code: Option[String],
                          hotspot_city_name: Option[String],
                          hotspot_country_code: Option[String],
                          hotspot_ident_code: Option[String],
                          user_provider: Option[String],
                          account_type_id: Option[String],
                          login_type: Option[String],
                          login_error_code: Option[String],
                          login_attempt_ts: Option[Long],
                          tid: Option[String]
                        )
  object FailedLogin {
    def apply(line: String): FailedLogin = {
      val arr = line.split(";", -1)
      new FailedLogin(
        data_code = Some(arr(0)),
        hotspot_provider_code = Some(arr(1)),
        hotspot_venue_code = Some(arr(2)),
        hotspot_venue_type_code = Some(arr(3)),
        hotspot_city_name = Some(arr(4)),
        hotspot_country_code = Some(arr(5)),
        hotspot_ident_code = Some(arr(6)),
        user_provider = Some(arr(7)),
        account_type_id = Some(arr(8)),
        login_type = Some(arr(9)),
        login_error_code = Some(arr(10)),
        login_attempt_ts = Some(arr(11).toInt),
        tid = Some(arr(12))
      )
    }
  }

}
