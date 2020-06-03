#!/bin/bash
hdfs dfs -rm -r \
hotspot_prototype/stage/cptm_ta_f_wlan_orderdb \
hotspot_prototype/stage/cptm_ta_f_wlif_map_voucher \
hotspot_prototype/stage/cptm_ta_q_wlan_cdr \
hotspot_prototype/stage/cptm_ta_x_wlan_session_d \
hotspot_prototype/stage/cptm_ta_q_wlan_cdr \
hotspot_prototype/stage/cptm_ta_x_wlan_session_q \
hotspot_prototype/stage/cptm_ta_x_wlan_orderdb_h \
hotspot_prototype/stage/cptm_ta_x_wlan_failed_login \

hdfs dfs -rm -r hotspot_prototype/stage/cptm_ta_x_wlan_failed_transac/date=202005*