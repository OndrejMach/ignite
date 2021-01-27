#!/bin/bash
export $PATH="/data/jobs/evl/prod/gcc6/bin:/data/jobs/evl/prod/ewhr/evl-2.1.0/bin/core:/data/jobs/evl/prod/ewhr/evl-2.1.0/bin:/data/jobs/evl/prod/ewhr/evl-2.1.0/3rdparty/oracle:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/dell/srvadmin/bin:/home/talendssing_datei/.local/bin:/home/talend_ewhr/bin"

LOG="-Dlog4j.configuration=file:log4j.properties"

#export input_date=`date +'%Y%m%d'`
export current_date=`date +'%Y%m%d'`
export current_processing_date=`date -d"$current_date - 1 day" "+%Y%m%d"`
export DES_encoder_path="./a.out"
#Input Paths
export input_folder="/user/talend_ewhr/rcse/input/"
#stage paths
export stage_folder="/user/talend_ewhr/rcse/stage/"
#Output paths
export output_folder="/user/talend_ewhr/rcse/out/"
#ORDERDB='/data/ewhr/raw/stage/hotspot/cptm_ta_f_wlan_orderdb.*.csv'
export app_home="/home/talend_ewhr/rcse_spark/"
export tmp="${app_home}tmp/"

cd $app_home

cat /data/input/ewhr/archive/rcse/TMD_*${current_processing_date}.csv.gz|gunzip |cut -f 2 -d '|'|sort -u >msisdn.csv
cat /data/input/ewhr/archive/rcse/TMD_*${current_processing_date}.csv.gz|gunzip |cut -f 3 -d '|'|sort -u >imsi.csv

./a.out msisdn.csv > msisdn_encoded.csv
./a.out imsi.csv > imsi_encoded.csv


hdfs dfs -rm ${input_folder}/*
hdfs dfs -put /data/input/ewhr/archive/rcse/TMD_*${current_processing_date}* $input_folder
hdfs dfs -put msisdn_encoded.csv $input_folder
hdfs dfs -put imsi_encoded.csv $input_folder

PARAMS="-Dconfig.processingDate=$current_processing_date \
-Dconfig.inputFilesPath=${input_folder}TMD_* \
-Dconfig.master=yarn \
-Dconfig.stageFiles.stagePath=$stage_folder \
-Dconfig.stageFiles.imsisEncodedPath=${input_folder}imsi_encoded.csv \
-Dconfig.stageFiles.msisdnsEncodedPath=${input_folder}msisdn_encoded.csv \
-Dconfig.outputs.outputPath=$output_folder $LOG"

/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --num-executors 8 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.rcse.Application ignite-1.0-all.jar terminalD

/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --num-executors 8 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.rcse.Application ignite-1.0-all.jar stage

/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --num-executors 8 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.rcse.Application ignite-1.0-all.jar aggregates

/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --num-executors 8 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.rcse.Application ignite-1.0-all.jar output


cd $tmp
hdfs dfs -get ${output_folder}*
scp -r * hadoop@10.105.178.122:/RCSe/
ARCHIVE_FILE="${processing_date}.tar.gz"
tar cvfz $ARCHIVE_FILE *
mv $ARCHIVE_FILE ../archive/
rm -r ${tmp}*
hdfs dfs -rm -r ${output_folder}/*
cd ..

