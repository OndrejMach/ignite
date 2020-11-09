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
    dag_id='SIT_inflight_monthly_reports',
    default_args=default_args,
    description='SIT_inflight_monthly_reports',
    start_date=datetime(2017, 3, 20),
    schedule_interval = '10 04 3 * *',
    catchup=False)



mail_to = "ondrej.machacek@external.t-mobile.cz, alena.preradova@t-mobile.cz"

VOUCHER='/user/talend_ewhr/hotspot_prototype/stage/cptm_ta_f_wlif_map_voucher'
ORDERDB='/user/talend_ewhr/hotspot_prototype/stage/cptm_ta_f_wlan_orderdb'
EXCHANGE_RATES='/user/talend_ewhr/common/cptm_ta_t_exchange_rates'
T_TABLE_MASK='/data/ewhr/raw/stage/inflight/cptm_ta_t_wlif_vchr_rds.*.csv'

OUTPUT='/user/talend_ewhr/inflight_prototype/output/'
INPUT='/user/talend_ewhr/inflight_prototype/input/'
STAGE='/user/talend_ewhr/inflight_prototype/stage/'
EXCEL_REPORT_PATH='/user/talend_ewhr/inflight_prototype/output/reports/'

WORK_FOLDER='/data/jobs/sit/inflight/'
TMP_FOLDER='{}tmp/'.format(WORK_FOLDER)

REPORT_SESSION="{}session.parquet".format(STAGE)
REPORT_COMPLETE="{}complete.parquet".format(STAGE)



DATE_NOW=datetime.today()
DATE_YESTERDAY = DATE_NOW - timedelta(days=1)

ARCHIVE_TODAY = "/data/input/ewhr/archive/inflight/G_{}*".format(DATE_NOW.strftime("%Y-%m-%d"))
ARCHIVE_YESTERDAY = "/data/input/ewhr/archive/inflight/G_{}*".format(DATE_YESTERDAY.strftime("%Y-%m-%d"))

MONTHLY_REPORT = datetime.strftime(DATE_NOW.replace(day=1) - timedelta(days=1),"%Y-%m-%d %H:%M:%S")

kinit_command = "/opt/shared/runtime/anaconda3/airconda/bin/kinit talend_ewhr@CDRS.TELEKOM.DE -k -t /home/talend_ewhr/talend_ewhr.keytab"

log_options= "-Dlog4j.configuration=file:{}log4j.properties".format(WORK_FOLDER)

def getParams(processingDate) :
    processingDateFormated=processingDate.strftime("%Y-%m-%d")
    FIRST_DATE = datetime.strftime(processingDate - timedelta(1), "%Y-%m-%d %H:%M:%S")
    FIRST_PLUS = datetime.strftime(processingDate, "%Y-%m-%d %H:%M:%S")
    OUTPUT_DATE = datetime.strftime(processingDate - timedelta(1), "%Y-%m-%d")
    T_TABLE_OUT="/user/talend_ewhr/inflight_prototype/output/cptm_ta_t_wlif_vchr_rds.{}.csv".format(OUTPUT_DATE)


    ret="-Dconfig.processingDateInput={} ".format(processingDateFormated) + \
        "-Dconfig.input.path={} ".format(INPUT) + \
        "-Dconfig.master=yarn "+ \
        "-Dconfig.stageFiles.voucherfile={} ".format(VOUCHER) + \
        "-Dconfig.stageFiles.orderDBFile={} ".format(ORDERDB) + \
        "-Dconfig.stageFiles.exchangeRatesFile={} ".format(EXCHANGE_RATES)+ \
        "-Dconfig.stageFiles.tFileMask={} ".format(T_TABLE_MASK) + \
        "-Dconfig.stageFiles.tFileStage={} ".format(T_TABLE_OUT) + \
        "-Dconfig.stageFiles.path=\'\' " + \
        "-Dconfig.output.path={} ".format(OUTPUT) + \
        "-Dconfig.firstDate=\'{}\' ".format(FIRST_DATE) + \
        "-Dconfig.firstPlus1Date=\'{}\' ".format(FIRST_PLUS) + \
        "-Dconfig.stageFiles.sessionFile={} ".format(REPORT_SESSION) + \
        "-Dconfig.stageFiles.completeFile={} ".format(REPORT_COMPLETE) + \
        "-Dconfig.monthlyReportDate=\'{}\' ".format(MONTHLY_REPORT) + \
        "-Dconfig.output.excelReportsPath={} ".format(EXCEL_REPORT_PATH) + \
        "-Dconfig.outputDate={} {} ".format(OUTPUT_DATE, log_options)

    return ret

def getSparkSubmit(date):
    params = str(getParams(date))

    ret = "/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --deploy-mode client " + \
          "--driver-java-options=\"{}\" --conf spark.executor.extraJavaOptions=\"{}\" --files {}/log4j.properties ".format(params,log_options,WORK_FOLDER) + \
          "--class com.tmobile.sit.ignite.inflight.Application {}ignite-1.0-all.jar monthly".format(WORK_FOLDER)

    return ret

spark_submit_template_today =str(getSparkSubmit(DATE_NOW))

get_outputs_cmd = "cd {} && hdfs dfs -get {}*".format(TMP_FOLDER, OUTPUT)

deliver_report_cmd = "mailx -a {} -s \"inflight_report_monthly\" {} </dev/null".format(TMP_FOLDER+"reports/*", mail_to)

clean_cmd = "hdfs dfs -rm -r {}/* && rm -r {}/*".format(OUTPUT, TMP_FOLDER)
#hdfs dfs -rm $INPUT*

#export PROCESSING_DATE=`date -d"$PROCESSING_DATE - 1 day" "+%Y-%m-%d"`
#hdfs dfs -put /data/input/ewhr/archive/inflight/G_$PROCESSING_DATE* $INPUT
#run_processing

kinit= BashOperator(task_id='kinit',
                    bash_command=kinit_command,
                    queue='EVL_Queue',
                    dag=dag)


report_processing = BashOperator(task_id='report_processing',
                                      bash_command=spark_submit_template_today,
                                      queue='EVL_Queue',
                                      dag=dag)

get_outputs = BashOperator(task_id='get_outputs',
                           bash_command=get_outputs_cmd,
                           queue='EVL_Queue',
                           dag=dag)
deliver_reports = BashOperator(task_id='deliver_reports',
                               bash_command=deliver_report_cmd,
                               queue='EVL_Queue',
                               dag=dag)

clean = BashOperator(task_id='clean',
                     bash_command=clean_cmd,
                     queue='EVL_Queue',
                     dag=dag)


kinit >> report_processing >> get_outputs >>deliver_reports >>clean