from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'kr_prod_airflow_operation_ewhr',
    'run_as_user': 'talend_ewhr',
    'start_date': datetime(2020, 2, 18),
    'retries': 0
}

dag = DAG(
    dag_id='SIT_hotspot_processing_daily',
    default_args=default_args,
    description='SIT_hotspot_processing_daily',
    start_date=datetime(2017, 3, 20),
    schedule_interval = '10 02 * * *',
    catchup=False)

input_date=datetime.today()
input_date_formated = input_date.strftime("%Y%m%d")
current_date=datetime.today()         #`date +'%Y%m%d'`
current_processing_date=current_date - timedelta(days=1)  # `date -d"$current_date - 1 day" "+%Y%m%d"`
DES_encoder_path="./a.out"
#Input Paths
input_folder="/user/talend_ewhr/hotspot_prototype/input/"
#stage paths
stage_folder="/user/talend_ewhr/hotspot_prototype/stage/"
#Output paths
output_folder="/user/talend_ewhr/hotspot_prototype/out/"

archive_folder="/data/input/ewhr/archive/hotspot/"

app_home="/home/talend_ewhr/hotspot_spark_app/"
tmp="{}tmp/".format(app_home)


kinit_command = "/opt/shared/runtime/anaconda3/airconda/bin/kinit talend_ewhr@CDRS.TELEKOM.DE -k -t /home/talend_ewhr/talend_ewhr.keytab"

log_options= "-Dlog4j.configuration=file:{}log4j.properties".format(app_home)


def getParams(processingDate) :
    processingDate_formated=processingDate.strftime("%Y%m%d")



    ret = "-Dconfig.processing_date={}".format(processingDate_formated) + \
        "-Dconfig.input_date={} ".format(input_date_formated) + \
        "-Dconfig.3DES_encoder_path={} ".format(DES_encoder_path) + \
        "-Dconfig.input.input_folder={} ".format(input_folder) + \
        "-Dconfig.master=yarn " + \
        "-Dconfig.stage.stage_folder={} ".format(stage_folder) + \
        "-Dconfig.stage.exchange_rates_filename=/user/talend_ewhr/common/cptm_ta_t_exchange_rates " + \
        "-Dconfig.output.output_folder={} {}".format(output_folder, log_options)

    return ret

def getSparkSubmit(date, regime):
    params = str(getParams(date))

    ret = "/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --deploy-mode client " + \
          "--driver-java-options=\"{}\" --conf spark.executor.extraJavaOptions=\"{}\" --files {}/log4j.properties ".format(params,log_options,app_home) + \
          "--class com.tmobile.sit.ignite.hotspot.Application ignite-1.0-all.jar {} ".format(regime)

    return ret


put_input_files_cmd = "hdfs dfs -put {}TMO.MPS.DAY.*{}* {}".format(archive_folder, input_date_formated, input_folder) + " && " + \
    "hdfs dfs -put {}TMO.CDR.DAY.*{}* {}".format(archive_folder, input_date_formated, input_folder) + " && " + \
    "hdfs dfs -put {}TMO.FAILEDLOGINS.DAY.*{}* {}".format(archive_folder,current_processing_date.strftime("%Y%m%d"), input_folder ) + " && " + \
    "hdfs dfs -put {}TMO.FAILEDLOGINS.DAY.*{}* {}".format(archive_folder,input_date_formated, input_folder )


input_processing_cmd = getSparkSubmit(current_processing_date, "input")

core_processing_4_stage_cmd = getSparkSubmit(current_processing_date - timedelta(days=4) , "stage")

core_processing_3_stage_cmd = getSparkSubmit(current_processing_date - timedelta(days=3) , "stage")

core_processing_2_stage_cmd = getSparkSubmit(current_processing_date - timedelta(days=2) , "stage")

core_processing_1_stage_cmd = getSparkSubmit(current_processing_date - timedelta(days=1) , "stage")

core_processing_0_stage_cmd = getSparkSubmit(current_processing_date , "stage")

core_processing_4_output_cmd = getSparkSubmit(current_processing_date - timedelta(days=4) , "output")

core_processing_3_output_cmd = getSparkSubmit(current_processing_date - timedelta(days=3) , "output")

core_processing_2_output_cmd = getSparkSubmit(current_processing_date - timedelta(days=2) , "output")

core_processing_1_output_cmd = getSparkSubmit(current_processing_date - timedelta(days=1) , "output")

core_processing_0_output_cmd = getSparkSubmit(current_processing_date , "output")


get_outputs_cmd = "cd {}".format(tmp) + " && " + "hdfs dfs -get ${}*".format(output_folder)

deliver_qv_cmd = "cd {} && scp -r *.csv hadoop@10.105.178.122:/WLAN/".format(tmp)

clean_cmd = "hdfs dfs -rm -r {}* && rm -r {}/*".format(output_folder, tmp) + " && " + "hdfs dfs -rm {}*".format(input_folder)
#hdfs dfs -rm $INPUT*

#export PROCESSING_DATE=`date -d"$PROCESSING_DATE - 1 day" "+%Y-%m-%d"`
#hdfs dfs -put /data/input/ewhr/archive/inflight/G_$PROCESSING_DATE* $INPUT
#run_processing

kinit = BashOperator(task_id='kinit',
                     bash_command=kinit_command,
                     queue='EVL_Queue',
                     dag=dag)


hdfs_put_input = BashOperator(task_id='hdfs_put_input',
                                  bash_command=put_input_files_cmd,
                                  queue='EVL_Queue',
                                  dag=dag)



input_processing = BashOperator(task_id='input_processing',
                                          bash_command=input_processing_cmd,
                                          queue='EVL_Queue',
                                          dag=dag)


stage_past_4 = BashOperator(task_id='stage_past_4',
                              bash_command=core_processing_4_stage_cmd,
                              queue='EVL_Queue',
                              dag=dag)

stage_past_3 = BashOperator(task_id='stage_past_3',
                                      bash_command=core_processing_3_stage_cmd,
                                      queue='EVL_Queue',
                                      dag=dag)

stage_past_2= BashOperator(task_id='stage_past_2',
                           bash_command=core_processing_2_stage_cmd,
                           queue='EVL_Queue',
                           dag=dag)

stage_past_1= BashOperator(task_id='stage_past_1',
                           bash_command=core_processing_1_stage_cmd,
                           queue='EVL_Queue',
                           dag=dag)

stage_today= BashOperator(task_id='stage_today',
                           bash_command=core_processing_0_stage_cmd,
                           queue='EVL_Queue',
                           dag=dag)

output_past_4 = BashOperator(task_id='output_past_4',
                            bash_command=core_processing_4_output_cmd,
                            queue='EVL_Queue',
                            dag=dag)

output_past_3 = BashOperator(task_id='output_past_3',
                             bash_command=core_processing_3_output_cmd,
                             queue='EVL_Queue',
                             dag=dag)

output_past_2 = BashOperator(task_id='output_past_2',
                             bash_command=core_processing_2_output_cmd,
                             queue='EVL_Queue',
                             dag=dag)

output_past_1 = BashOperator(task_id='output_past_1',
                             bash_command=core_processing_1_output_cmd,
                             queue='EVL_Queue',
                             dag=dag)

output_today = BashOperator(task_id='output_today',
                             bash_command=core_processing_0_output_cmd,
                             queue='EVL_Queue',
                             dag=dag)

deliver_qv = BashOperator(task_id='deliver_qv',
                          bash_command=deliver_qv_cmd,
                          queue='EVL_Queue',
                          dag=dag)

clean = BashOperator(task_id='clean',
                     bash_command=clean_cmd,
                     queue='EVL_Queue',
                     dag=dag)


kinit >> hdfs_put_input >> input_processing >> stage_past_4 >> stage_past_3 >> stage_past_2 >> stage_past_1 >>stage_today >> output_past_4 >> output_past_3 >> output_past_2 >> output_past_1 >> output_today >> deliver_qv >> clean


