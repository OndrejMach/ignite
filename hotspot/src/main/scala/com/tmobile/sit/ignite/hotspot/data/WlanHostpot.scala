package com.tmobile.sit.ignite.hotspot.data

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object WlanHotspotTypes {
  val wlanHotspotStructure = StructType(
    Seq(
      StructField("wlan_hotspot_id", LongType, true),
      StructField("wlan_hotspot_ident_code", StringType, true),
      StructField("wlan_hotspot_desc", StringType, true),
      StructField("wlan_hotspot_timezone", StringType, true),
      StructField("wlan_ip_range_start_dec", LongType, true),
      StructField("wlan_ip_range_end_dec", LongType, true),
      StructField("wlan_ip_range_start", StringType, true),
      StructField("wlan_ip_range_end", StringType, true),
      StructField("wlan_hotspot_status", StringType, true),
      StructField("wlan_venue_type_code", StringType, true),
      StructField("wlan_venue_code", StringType, true),
      StructField("wlan_provider_code", StringType, true),
      StructField("wlan_hotspot_area_code", StringType, true),
      StructField("ssid", StringType, true),
      StructField("country_code", StringType, true),
      StructField("city_code", StringType, true),
      StructField("postcode", StringType, true),
      StructField("street", StringType, true),
      StructField("house_no", StringType, true),
      StructField("access_control", StringType, true),
      StructField("long_deg", StringType, true),
      StructField("long_min", StringType, true),
      StructField("long_sec", StringType, true),
      StructField("lat_deg", StringType, true),
      StructField("lat_min", StringType, true),
      StructField("lat_sec", StringType, true),
      StructField("open_monday", StringType, true),
      StructField("open_tuesday", StringType, true),
      StructField("open_wednesday", StringType, true),
      StructField("open_thursday", StringType, true),
      StructField("open_friday", StringType, true),
      StructField("open_saturday", StringType, true),
      StructField("open_sunday", StringType, true),
      StructField("open_comment", StringType, true),
      StructField("parking_available", StringType, true),
      StructField("loc_description", StringType, true),
      StructField("loc_spec_offer", StringType, true),
      StructField("loc_url", StringType, true),
      StructField("loc_cont_tel", StringType, true),
      StructField("loc_cont_fax", StringType, true),
      StructField("loc_cont_email", StringType, true),
      StructField("ap_amount", StringType, true),
      StructField("coord_system", StringType, true),
      StructField("commercial_status", StringType, true),
      StructField("parent_ident_code", StringType, true),
      StructField("bandwidth", StringType, true),
      StructField("valid_from", TimestampType, true),
      StructField("valid_to", TimestampType, true),
      StructField("entry_id", LongType, true),
      StructField("load_date", TimestampType, true)
    )
  )


  case class WlanHostpot(
                          wlan_hotspot_id: Option[Long],
                          wlan_hotspot_ident_code: Option[String],
                          wlan_hotspot_desc: Option[String],
                          wlan_hotspot_timezone: Option[String],
                          wlan_ip_range_start_dec: Option[Long],
                          wlan_ip_range_end_dec: Option[Long],
                          wlan_ip_range_start: Option[String],
                          wlan_ip_range_end: Option[String],
                          wlan_hotspot_status: Option[String],
                          wlan_venue_type_code: Option[String],
                          wlan_venue_code: Option[String],
                          wlan_provider_code: Option[String],
                          wlan_hotspot_area_code: Option[String],
                          ssid: Option[String],
                          country_code: Option[String],
                          city_code: Option[String],
                          postcode: Option[String],
                          street: Option[String],
                          house_no: Option[String],
                          access_control: Option[String],
                          long_deg: Option[String],
                          long_min: Option[String],
                          long_sec: Option[String],
                          lat_deg: Option[String],
                          lat_min: Option[String],
                          lat_sec: Option[String],
                          open_monday: Option[String],
                          open_tuesday: Option[String],
                          open_wednesday: Option[String],
                          open_thursday: Option[String],
                          open_friday: Option[String],
                          open_saturday: Option[String],
                          open_sunday: Option[String],
                          open_comment: Option[String],
                          parking_available: Option[String],
                          loc_description: Option[String],
                          loc_spec_offer: Option[String],
                          loc_url: Option[String],
                          loc_cont_tel: Option[String],
                          loc_cont_fax: Option[String],
                          loc_cont_email: Option[String],
                          ap_amount: Option[String],
                          coord_system: Option[String],
                          commercial_status: Option[String],
                          parent_ident_code: Option[String],
                          bandwidth: Option[String],
                          valid_from: Option[Timestamp],
                          valid_to: Option[Timestamp]
                          //entry_id: Option[Long],
                          //load_date: Option[Timestamp]
                        )
/*
  object WlanHostpot{

    def apply(stage: WlanHotspotStage): WlanHostpot = {
      def getValue(s: Option[String]) : Option[String] = if (s.isDefined && !s.get.isEmpty) s else Some("UNDEFINED")
      
      new WlanHostpot(
        wlan_hotspot_id= stage.hotspot_id,
        wlan_hotspot_ident_code= stage.hotspot_ident_code,
        wlan_hotspot_desc=Some("Hotspot not assigned"),
        wlan_hotspot_timezone= stage.hotspot_timezone,
        wlan_ip_range_start_dec=None,
        wlan_ip_range_end_dec=None,
        wlan_ip_range_start=None,
        wlan_ip_range_end=None,
        wlan_hotspot_status=None,
        wlan_venue_type_code= getValue(stage.hotspot_venue_type_code),
        wlan_venue_code=getValue(stage.hotspot_venue_code),
        wlan_provider_code=stage.hotspot_provider_code,
        wlan_hotspot_area_code=None,
        ssid=None,
        country_code=stage.hotspot_country_code,
        city_code=getValue(stage.hotspot_city_code),
        postcode=None,
        street=None,
        house_no=None,
        access_control=None,
        long_deg=None,
        long_min=None,
        long_sec=None,
        lat_deg=None,
        lat_min=None,
        lat_sec=None,
        open_monday=None,
        open_tuesday=None,
        open_wednesday=None,
        open_thursday=None,
        open_friday=None,
        open_saturday=None,
        open_sunday=None,
        open_comment=None,
        parking_available=None,
        loc_description=None,
        loc_spec_offer=None,
        loc_url=None,
        loc_cont_tel=None,
        loc_cont_fax=None,
        loc_cont_email=None,
        ap_amount=None,
        coord_system=None,
        commercial_status=None,
        parent_ident_code=None,
        bandwidth=None,
        valid_from= stage.valid_from,
        valid_to=stage.valid_to,
        entry_id=Some(0),
        load_date=Some(Timestamp.valueOf(LocalDateTime.now()))
      )
    }


  }

 */

  case class WlanHotspotStage(
                               //hotspot_id: Option[Long],
                               hotspot_ident_code: Option[String],
                               hotspot_timezone: Option[String],
                               hotspot_venue_type_code: Option[String],
                               hotspot_venue_code: Option[String],
                               hotspot_provider_code: Option[String],
                               hotspot_country_code: Option[String],
                               hotspot_city_code: Option[String],
                               valid_from_n: Option[Timestamp],
                               valid_to_n: Option[Timestamp]
                             )
}