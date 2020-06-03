#!/bin/bash

LOG="-Dlog4j.configuration=file:log4j.properties"

VOUCHER='/data/ewhr/raw/stage/inflight/cptm_ta_f_wlif_map_voucher.*.csv'
ORDERDB='/data/ewhr/raw/stage/hotspot/cptm_ta_f_wlan_orderdb.*.csv'
EXCHANGE_RATES='/data/ewhr/raw/stage/common/cptm_ta_t_exchange_rates.csv'

OUTPUT='/user/talend_ewhr/inflight_prototype/output/'
INPUT='/user/talend_ewhr/inflight_prototype/input/'

PROCESSING_DATE=`date +'%Y-%m-%d'`

run_processing () {
FIRST_DATE=`date -d"$PROCESSING_DATE - 1 day" "+%Y-%m-%d %H:%M:%S"`
FIRST_PLUS=`date -d"$FIRST_DATE + 1 day" "+%Y-%m-%d %H:%M:%S"`

echo "Copying input files to HDFS"

hdfs dfs -put /data/input/ewhr/archive/inflight/G_$PROCESSING_DATE* $INPUT

echo "Executing processing"

PARAMS="-Dconfig.processingDateInput=$PROCESSING_DATE \
-Dconfig.input.path=$INPUT \
-Dconfig.stageFiles.voucherfile=$VOUCHER \
-Dconfig.stageFiles.orderDBFile=$ORDERDB \
-Dconfig.stageFiles.exchangeRatesFile=$EXCHANGE_RATES \
-Dconfig.stageFiles.path='' \
-Dconfig.output.path=$OUTPUT \
-Dconfig.firstDate=\"$FIRST_DATE\" \
-Dconfig.firstPlus1Date=\"$FIRST_PLUS\" \
-Dconfig.outputDate=$PROCESSING_DATE $LOG"

echo $PARAMS

spark2-submit --master yarn --num-executors 50 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties" --class com.tmobile.sit.ignite.inflight.Application ignite-1.0-all.jar

}

hdfs dfs -rm $INPUT*

export PROCESSING_DATE=`date -d"$PROCESSING_DATE - 1 day" "+%Y-%m-%d"`
run_processing

PROCESSING_DATE=`date +'%Y-%m-%d'`
run_processing
