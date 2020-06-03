#!/bin/bash
export $PATH="/data/jobs/evl/prod/gcc6/bin:/data/jobs/evl/prod/ewhr/evl-2.1.0/bin/core:/data/jobs/evl/prod/ewhr/evl-2.1.0/bin:/data/jobs/evl/prod/ewhr/evl-2.1.0/3rdparty/oracle:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/dell/srvadmin/bin:/home/talend_ewhr/.local/bin:/home/talend_ewhr/bin"

RUN_MODE=$1

LOG="-Dlog4j.configuration=file:log4j.properties"

current_date=`date +'%Y%m%d'`
processing_date=`date -d"$PROCESSING_DATE - 1 day" "+%Y%m%d"`
DES_encoder_path="./a.out"
#Input Paths
input_folder="/user/talend_ewhr/hotspot_prototype/input/"
#stage paths
stage_folder="/user/talend_ewhr/hotspot_prototype/stage/"
#Output paths
output_folder="/user/talend_ewhr/hotspot_prototype/out/"
#ORDERDB='/data/ewhr/raw/stage/hotspot/cptm_ta_f_wlan_orderdb.*.csv'
tmp="/home/talend_ewhr/hotspot_spark_app/tmp/"

run_processing () {
echo "Executing processing in run mode $RUN_MODE"

PARAMS="-Dconfig.processing_date=$processing_date \
-Dconfig.input_date=$input_date \
-Dconfig.3DES_encoder_path=$DES_encoder_path \
-Dconfig.input.input_folder=$input_folder \
-Dconfig.stage.stage_folder=$stage_folder \
-Dconfig.output.output_folder=$output_folder $LOG"

echo $PARAMS

/usr/bin/spark2-submit --master yarn --num-executors 2 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.hotspot.Application ignite-1.0-all.jar input

/usr/bin/spark2-submit --master yarn --num-executors 2 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.hotspot.Application ignite-1.0-all.jar stage

/usr/bin/spark2-submit --master yarn --num-executors 2 --executor-cores 8 --executor-memory 20G --driver-memory 20G \
 --driver-java-options="$PARAMS" --conf "spark.executor.extraJavaOptions=$LOG" --files "./log4j.properties,./a.out" --class com.tmobile.sit.ignite.hotspot.Application ignite-1.0-all.jar output

}

hdfs dfs -rm $stage_folder/cptm_ta_t_exchange_rates.csv
hdfs dfs -cp /data/ewhr/raw/stage/common/cptm_ta_t_exchange_rates.csv $stage_folder

for i in 4 3 2 1 0
do
  hdfs dfs -rm ${input_folder}/*


  export input_date=`date -d"$current_date - $i day" "+%Y%m%d"`
  export processing_date=`date -d"$input_date - 1 day" "+%Y%m%d"`
  hdfs dfs -put /data/input/ewhr/archive/hotspot/TMO*$input_date* $input_folder
  echo "running processing for input date $input_date, processing date $processing_date"
  run_processing

  echo "Transfering files to the SFTP server"

  cd $tmp
  hdfs dfs -get ${output_folder}*
  scp -r * hadoop@10.105.178.122:/WLAN/SCALA_Test_Data/
  ARCHIVE_FILE="${processing_date}-${i}.tar.gz"
  tar cvfz $ARCHIVE_FILE *
  mv $ARCHIVE_FILE ../archive/
  rm -r ${tmp}*
  hdfs dfs -rm -r ${output_folder}/*
  cd ..
done
