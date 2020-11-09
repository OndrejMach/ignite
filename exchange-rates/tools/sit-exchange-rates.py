from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow import DAG
from datetime import date

default_args = {
    'owner': 'kr_prod_airflow_operation_ewhr',
    'run_as_user': 'talend_ewhr',
    'start_date': datetime(2020, 2, 18),
    'schedule_interval' : "10 02 * * *",
    'retries': 0
}

dag = DAG(
    dag_id='SIT_exchange_rates_processing',
    default_args=default_args,
    description='SIT_exchange_rates_processing',
    start_date=datetime(2017, 3, 20),
    catchup=False)

processing_date = date.today().strftime("%Y%m%d")
input_folder="/user/talend_ewhr/exchange_rates/input/"
stage_folder="/user/talend_ewhr/common/"
input_data = "/data/input/ewhr/archive/common/"
app_home = "/home/talend_ewhr/exchange_rates_spark_app/"


kinit_command = "/opt/shared/runtime/anaconda3/airconda/bin/kinit talend_ewhr@CDRS.TELEKOM.DE -k -t /home/talend_ewhr/talend_ewhr.keytab"


log_options= "-Dlog4j.configuration=file:{}log4j.properties".format(app_home)

params="-Dconfig.input_date={} ".format(processing_date) + \
"-Dconfig.input.input_folder={} ".format(input_folder) + \
"-Dconfig.master=yarn " + \
"-Dconfig.stage.stage_folder={} {}".format(stage_folder, log_options)

spark_submit_template =kinit_command+" && "+ "/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --deploy-mode client " + \
                       "--driver-java-options=\"{}\" --conf spark.executor.extraJavaOptions=\"{}\" --files {}/log4j.properties ".format(params,log_options,app_home) + \
                       "--class com.tmobile.sit.ignite.exchangerates.Application {}ignite-1.0-all.jar ".format(app_home)


hdfs_put = BashOperator(task_id='hdfs_put',
                        bash_command='{} && hdfs dfs -rm {}/* && hdfs dfs -put {}CUP_exchangerates_d_*{}*.csv.gz {} && hdfs dfs -ls {}'.format(kinit_command, input_folder, input_data, processing_date, input_folder,input_folder ),
                        queue='EVL_Queue',
                        dag=dag)



input_processing = BashOperator(task_id='input_processing',
                                bash_command=spark_submit_template,
                                queue='EVL_Queue',
                                dag=dag)

hdfs_put >> input_processing