#!/bin/bash
export $PATH="/data/jobs/evl/prod/gcc6/bin:/data/jobs/evl/prod/ewhr/evl-2.1.0/bin/core:/data/jobs/evl/prod/ewhr/evl-2.1.0/bin:/data/jobs/evl/prod/ewhr/evl-2.1.0/3rdparty/oracle:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/dell/srvadmin/bin:/home/talendssing_datei/.local/bin:/home/talend_ewhr/bin"

LOG="-Dlog4j.configuration=file:log4j.properties"

#export input_date=`date +'%Y%m%d'`
export current_date=`date +'%Y%m%d'`
#Input Paths
export input_folder="/user/talend_ewhr/exchange_rates/input/"
#stage paths
export stage_folder="/user/talend_ewhr/common/"
export app_home="/home/talend_ewhr/exchange_rates_spark_app/"
#/home/talend_ewhr/exchange_rates_spark_app
cd $app_home

echo "running processing for $current_processing_date"

#CUP_exchangerates_d_20200727_1.csv.gz
hdfs dfs -rm ${input_folder}/*
hdfs dfs -put /data/input/ewhr/archive/common/CUP_exchangerates_d_*${current_date}*.csv.gz $input_folder

PARAMS="-Dconfig.input_date=$current_date \
-Dconfig.input.input_folder=$input_folder \
-Dconfig.master=yarn \
-Dconfig.stage.stage_folder=$stage_folder $LOG"

/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --num-executors 8 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties" --class com.tmobile.sit.ignite.exchangerates.Application ignite-1.0-all.jar