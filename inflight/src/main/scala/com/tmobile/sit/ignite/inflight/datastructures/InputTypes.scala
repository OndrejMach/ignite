package com.tmobile.sit.ignite.inflight.datastructures

import java.sql.{Date, Timestamp}

object InputTypes {

  case class Aircraft(tailsign: Option[String],
                      manufacturer: Option[String],
                      ac_type: Option[String],
                      serial_number: Option[String],
                      year_of_manufacture: Option[Long],
                      name: Option[String],
                      airline: Option[String],
                      icao_type: Option[String],
                      iata_type: Option[String],
                      gcs_equipped: Option[String],
                      xid: Option[Long],
                      hotspot_id: Option[String])

  case class Airline(airline_name: Option[String],
                     airline_icao: Option[String],
                     airline_iata: Option[String],
                     airline_logo_file: Option[String]
                    )

  case class Airport(airport_icao: Option[String],
                     airport_iata: Option[String],
                     airport_name: Option[String],
                     airport_city: Option[String],
                     airport_country: Option[String],
                     airport_latitude: Option[Double],
                     airport_longitude: Option[Double],
                     airport_coverage: Option[String]
                    )


  case class AirportLeg(wlif_flight_id: Option[Long],
                        wlif_flightleg_status: Option[String],
                        wlif_airline_code: Option[String],
                        wlif_aircraft_code: Option[String],
                        wlif_flight_number: Option[String],
                        wlif_airport_code_origin: Option[String],
                        wlif_airport_code_destination: Option[String],
                        wlif_date_time_opened: Option[Timestamp],
                        wlif_method_opened: Option[String],
                        wlif_date_time_closed: Option[Timestamp],
                        wlif_method_closed: Option[String],
                        wlif_xid_pac: Option[Long],
                        wlif_num_users: Option[Long],
                        wlif_num_sessions: Option[Long],
                        wlif_session_time: Option[Long],
                        wlif_session_volume_out: Option[Double],
                        wlif_session_volume_in: Option[Double],
                        wlif_active_sessions: Option[Long]
                       )

  case class Oooid(
                    wlif_sequence: Option[Long],
                    wlif_method: Option[String],
                    wlif_flight_id: Option[Long],
                    wlif_auid: Option[String],
                    wlif_xid_pac: Option[Long],
                    wlif_airline_code: Option[String],
                    wlif_aircraft_code: Option[String],
                    wlif_flight_number: Option[String],
                    wlif_airport_code_origin: Option[String],
                    wlif_airport_code_destination: Option[String],
                    wlif_date_time_event: Option[Timestamp],
                    wlif_date_time_received: Option[Timestamp]
                  )

  case class Realm(
                    realm_prefix: Option[String],
                    account_type: Option[String]
                  )

  case class OrderDB(ta_id: Option[String],
                     ta_request_date: Option[Date],
                     ta_request_datetime: Option[Timestamp],
                     ta_request_hour: Option[String],
                     payid: Option[String],
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
                     entry_id: Long,
                     load_date: Timestamp
                    )

  case class MapVoucher(
                         wlan_ta_id: Option[String],
                         wlan_request_date: Option[Timestamp],
                         wlan_username: Option[String],
                         wlif_username: Option[String],
                         wlif_realm_code: Option[String],
                         entry_id: Option[Long],
                         load_date: Option[Timestamp]
                       )

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
                            valid_to: Option[Date],
                            entry_id: Option[Long],
                            load_date: Option[Timestamp]
                          )
}