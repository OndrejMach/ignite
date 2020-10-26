package com.tmobile.sit.ignite.deviceatlas.data

import org.apache.spark.sql.types._

object FileStructures {
  val deviceMap = StructType(
    Seq(
      StructField("tac", StringType, false),
      StructField("gsma_marketing_name", StringType, true),
      StructField("gsma_internal_model_name", StringType, true),
      StructField("gsma_manufacturer", StringType, true),
      StructField("gsma_bands", StringType, true),
      StructField("gsma_5Gbands", StringType, true),
      StructField("gsma_allocation_date", StringType, true),
      StructField("gsma_country_code", StringType, true),
      StructField("gsma_fixed_code", StringType, true),
      StructField("gsma_manufacturer_code", StringType, true),
      StructField("gsma_radio_interface", StringType, true),
      StructField("gsma_brand_name", StringType, true),
      StructField("gsma_model_name", StringType, true),
      StructField("gsma_operating_system", StringType, true),
      StructField("gsma_nfc", StringType, true),
      StructField("gsma_bluetooth", StringType, true),
      StructField("gsma_wlan", StringType, true),
      StructField("gsma_device_type", StringType, true),
      StructField("deviceatlas_id", IntegerType, true),
      StructField("standardised_full_name", StringType, true),
      StructField("standardised_device_vendor", StringType, true),
      StructField("standardised_device_model", StringType, true),
      StructField("standardised_marketing_name", StringType, true),
      StructField("manufacturer", StringType, true),
      StructField("year_released", IntegerType, true),
      StructField("mobile_device", StringType, true),
      StructField("primary_hardware_type", StringType, true),
      StructField("touch_screen", StringType, true),
      StructField("screen_width", IntegerType, true),
      StructField("screen_height", IntegerType, true),
      StructField("diagonal_screen_size", StringType, true),
      StructField("display_ppi", IntegerType, true),
      StructField("device_pixel_ratio", StringType, true),
      StructField("screen_color_depth", StringType, true),
      StructField("nfc", StringType, true),
      StructField("camera", StringType, true),
      StructField("is_mobile_phone", StringType, true),
      StructField("is_tablet", StringType, true),
      StructField("is_ereader", StringType, true),
      StructField("is_games_console", StringType, true),
      StructField("is_tv", StringType, true),
      StructField("is_set_top_box", StringType, true),
      StructField("is_media_player", StringType, true),
      StructField("chipset_vendor", StringType, true),
      StructField("chipset_name", StringType, true),
      StructField("chipset_model", StringType, true),
      StructField("cpu_name", StringType, true),
      StructField("cpu_cores", StringType, true),
      StructField("cpu_maximum_frequency", StringType, true),
      StructField("gpu_name", StringType, true),
      StructField("sim_slots", IntegerType, true),
      StructField("esim_count", IntegerType, true),
      StructField("sim_size", StringType, true),
      StructField("internal_storage_capacity", StringType, true),
      StructField("expandable_storage", StringType, true),
      StructField("total_ram", StringType, true),
      StructField("os_name", StringType, true),
      StructField("os_version", StringType, true),
      StructField("os_android", StringType, true),
      StructField("os_bada", StringType, true),
      StructField("os_ios", StringType, true),
      StructField("os_rim", StringType, true),
      StructField("os_symbian", StringType, true),
      StructField("os_windows_mobile", StringType, true),
      StructField("os_windows_phone", StringType, true),
      StructField("os_windows_rt", StringType, true),
      StructField("os_web_os", StringType, true),
      StructField("browser_name", StringType, true),
      StructField("browser_version", StringType, true),
      StructField("browser_rendering_engine", StringType, true),
      StructField("markup_xhtml_basic_1_0", StringType, true),
      StructField("markup_xhtml_mp_1_0", StringType, true),
      StructField("markup_xhtml_mp_1_1", StringType, true),
      StructField("markup_xhtml_mp_1_2", StringType, true),
      StructField("markup_wml1", StringType, true),
      StructField("vcard_download", StringType, true),
      StructField("image_gif87", StringType, true),
      StructField("image_gif89a", StringType, true),
      StructField("image_jpg", StringType, true),
      StructField("image_png", StringType, true),
      StructField("uri_scheme_tel", StringType, true),
      StructField("uri_scheme_sms", StringType, true),
      StructField("uri_scheme_sms_to", StringType, true),
      StructField("cookie", StringType, true),
      StructField("https", StringType, true),
      StructField("memory_limit_markup", IntegerType, true),
      StructField("memory_limit_embedded_media", IntegerType, true),
      StructField("memory_limit_download", IntegerType, true),
      StructField("flash_capable", StringType, true),
      StructField("js_support_basic_java_script", StringType, true),
      StructField("js_modify_dom", StringType, true),
      StructField("js_modify_css", StringType, true),
      StructField("js_support_events", StringType, true),
      StructField("js_support_event_listener", StringType, true),
      StructField("js_xhr", StringType, true),
      StructField("js_support_console_log", StringType, true),
      StructField("js_json", StringType, true),
      StructField("supports_client_side", StringType, true),
      StructField("csd", StringType, true),
      StructField("hscsd", StringType, true),
      StructField("gprs", StringType, true),
      StructField("edge", StringType, true),
      StructField("hsdpa", StringType, true),
      StructField("umts", StringType, true),
      StructField("hspa", StringType, true),
      StructField("lte", StringType, true),
      StructField("lte_category", IntegerType, true),
      StructField("lte_advanced", StringType, true),
      StructField("volte", StringType, true),
      StructField("wi_fi", StringType, true),
      StructField("vowifi", StringType, true),
      StructField("2G", IntegerType, true),
      StructField("3G", IntegerType, true),
      StructField("4G", IntegerType, true),
      StructField("5G", IntegerType, true),
      StructField("html_audio", StringType, true),
      StructField("html_canvas", StringType, true),
      StructField("html_inline_svg", StringType, true),
      StructField("html_svg", StringType, true),
      StructField("html_video", StringType, true),
      StructField("css_animations", StringType, true),
      StructField("css_columns", StringType, true),
      StructField("css_transforms", StringType, true),
      StructField("css_transitions", StringType, true),
      StructField("js_application_cache", StringType, true),
      StructField("js_geo_location", StringType, true),
      StructField("js_indexeddb", StringType, true),
      StructField("js_local_storage", StringType, true),
      StructField("js_session_storage", StringType, true),
      StructField("js_web_gl", StringType, true),
      StructField("js_web_sockets", StringType, true),
      StructField("js_web_sql_database", StringType, true),
      StructField("js_web_workers", StringType, true),
      StructField("js_device_orientation", StringType, true),
      StructField("js_device_motion", StringType, true),
      StructField("js_touch_events", StringType, true),
      StructField("js_query_selector", StringType, true),
      StructField("wmv", StringType, true),
      StructField("midi_monophonic", StringType, true),
      StructField("midi_polyphonic", StringType, true),
      StructField("amr", StringType, true),
      StructField("mp3", StringType, true),
      StructField("aac", StringType, true)
    )
  )

  val manufacturer_Lkp = StructType(
    Seq(
      StructField("gsma_manufacturer",		    StringType),
      StructField("terminal_db_manufacturer",	StringType)
    )
  )

  val manufacturerVendor_Lkp = StructType(
    Seq(
      StructField("gsma_manufacturer",               StringType),
      StructField("gsma_standardised_device_vendor", StringType),
      StructField("terminal_db_manufacturer",        StringType)
    )
  )

  val operatingSystem_Lkp = StructType(
    Seq(
      StructField("terminaldb",     StringType),
      StructField("gsma_os_name",   StringType)
    )
  )

  val osNokia_Lkp = StructType(
    Seq(
      StructField("terminaldb",         StringType),
      StructField("gsma_os_version",    StringType)
    )
  )

  val terminalDB_Lkp = StructType(
    Seq(
      StructField("tac_code",       StringType),
      StructField("terminal_id",    IntegerType)
    )
  )

  val tacBlacklist_Lkp = StructType(
    Seq(
      StructField("tac", StringType)
    )
  )

  val terminalDB_full_lkp = StructType(
    Seq(
      StructField("tac_code"                     , StringType , false),
      StructField("id"                           , IntegerType, false),
      StructField("terminal_id"                  , IntegerType, false),
      StructField("manufacturer"                 , StringType , true),
      StructField("model"                        , StringType , true),
      StructField("model_alias"                  , StringType , true),
      StructField("terminal_full_name"           , StringType , true),
      StructField("csso_alias"                   , StringType , true),
      StructField("status"                       , StringType , true),
      StructField("international_material_number", StringType , true),
      StructField("launch_date"                  , DateType   , true),
      StructField("gsm_bandwidth"                , StringType , true),
      StructField("gprs_capable"                 , StringType , true),
      StructField("edge_capable"                 , StringType , true),
      StructField("umts_capable"                 , StringType , true),
      StructField("wlan_capable"                 , StringType , true),
      StructField("form_factor"                  , StringType , true),
      StructField("handset_tier"                 , StringType , true),
      StructField("wap_type"                     , StringType , true),
      StructField("wap_push_capable"             , StringType , true),
      StructField("colour_depth"                 , StringType , true),
      StructField("mms_capable"                  , StringType , true),
      StructField("camera_type"                  , StringType , true),
      StructField("camera_resolution"            , StringType , true),
      StructField("video_messaging_capable"      , StringType , true),
      StructField("video_record"                 , StringType , true),
      StructField("ringtone_type"                , StringType , true),
      StructField("java_capable"                 , StringType , true),
      StructField("email_client"                 , StringType , true),
      StructField("email_push_capable"           , StringType , true),
      StructField("operating_system"             , StringType , true),
      StructField("golden_gate_user_interface"   , StringType , true),
      StructField("tzones_hard_key"              , StringType , true),
      StructField("bluetooth_capable"            , StringType , true),
      StructField("tm3_capable"                  , StringType , true),
      StructField("wnw_device"                   , StringType , true),
      StructField("concept_class"                , StringType , true),
      StructField("price_tier"                   , StringType , true),
      StructField("integrated_music_player"      , StringType , true),
      StructField("gps"                          , StringType , true),
      StructField("wnw_browser_class"            , StringType , true),
      StructField("browser_type"                 , StringType , true),
      StructField("input_method"                 , StringType , true),
      StructField("resolution_main"              , StringType , true),
      StructField("display_size"                 , StringType , true),
      StructField("highest_upload"               , StringType , true),
      StructField("highest_download"             , StringType , true),
      StructField("lte"                          , StringType , true),
      StructField("display_type"                 , StringType , true),
      StructField("form_factor_detailed"         , StringType , true),
      StructField("operating_system_detailed"    , StringType , true),
      StructField("operating_system_version"     , StringType , true),
      StructField("browser_vendor"               , StringType , true),
      StructField("browser_version"              , StringType , true),
      StructField("browser_version_cat"          , StringType , true),
      StructField("app_store"                    , StringType , true),
      StructField("mvoip_possible_device"        , StringType , true),
      StructField("tm_smartphone_all"            , StringType , true),
      StructField("nfc_capable"                  , StringType , true),
      StructField("cpu_power"                    , StringType , true),
      StructField("ram"                          , StringType , true),
      StructField("master_terminal_id"           , StringType , true),
      StructField("notice_freetext"              , StringType , true),
      StructField("volte_capability"             , StringType , true)
    )
  )

  val terminal_id_lkp = StructType(
    Seq(
      StructField("id", LongType)
    )
  )
  val cptm_ta_d_tac = StructType(
    Seq(
      StructField("terminal_id"                   , IntegerType , true),
      StructField("tac_code"                      , StringType  , true),
      StructField("id"                            , IntegerType , true),
      StructField("manufacturer"                  , StringType  , true),
      StructField("model"                         , StringType  , true),
      StructField("model_alias"                   , StringType  , true),
      StructField("csso_alias"                    , StringType  , true),
      StructField("status"                        , StringType  , true),
      StructField("international_material_number" , StringType  , true),
      StructField("launch_date"                   , StringType    , true),
      StructField("gsm_bandwidth"                 , StringType  , true),
      StructField("gprs_capable"                  , StringType  , true),
      StructField("edge_capable"                  , StringType  , true),
      StructField("umts_capable"                  , StringType  , true),
      StructField("wlan_capable"                  , StringType  , true),
      StructField("form_factor"                   , StringType  , true),
      StructField("handset_tier"                  , StringType  , true),
      StructField("wap_type"                      , StringType  , true),
      StructField("wap_push_capable"              , StringType  , true),
      StructField("colour_depth"                  , StringType  , true),
      StructField("mms_capable"                   , StringType  , true),
      StructField("camera_type"                   , StringType  , true),
      StructField("camera_resolution"             , StringType  , true),
      StructField("video_messaging_capable"       , StringType  , true),
      StructField("ringtone_type"                 , StringType  , true),
      StructField("java_capable"                  , StringType  , true),
      StructField("email_client"                  , StringType  , true),
      StructField("email_push_capable"            , StringType  , true),
      StructField("operating_system"              , StringType  , true),
      StructField("golden_gate_user_interface"    , StringType  , true),
      StructField("tzones_hard_key"               , StringType  , true),
      StructField("bluetooth_capable"             , StringType  , true),
      StructField("tm3_capable"                   , StringType  , true),
      StructField("terminal_full_name"            , StringType  , true),
      StructField("video_record"                  , StringType  , true),
      StructField("valid_from"                    , StringType    , true),
      StructField("valid_to"                      , StringType    , true),
      StructField("entry_id"                      , LongType , true),
      StructField("load_date"                     , StringType    , true)
    )
  )

  val term_spec = StructType(
    Seq(
      StructField("terminal_id",         IntegerType, false),
      StructField("terminal_spec_name",  StringType , true),
      StructField("terminal_spec_value", StringType , true)
    )
  )

  val cptm_term_spec = StructType(
    Seq(
      StructField("terminal_id",         IntegerType, false),
      StructField("terminal_spec_name",  StringType , true),
      StructField("terminal_spec_value", StringType , true),
      StructField("valid_from",          StringType , true),
      StructField("valid_to",            StringType , true),
      StructField("entry_id",            IntegerType, true),
      StructField("load_date",           StringType , true)
    )
  )


}
