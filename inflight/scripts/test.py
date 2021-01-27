from datetime import datetime, timedelta


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

MONTHLY_REPORT = DATE_NOW.replace(day=1) - timedelta(days=1)

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
        "-Dconfig.firstDate=\\\"{}\\\" ".format(FIRST_DATE) + \
        "-Dconfig.firstPlus1Date=\\\"{}\\\" ".format(FIRST_PLUS) + \
        "-Dconfig.stageFiles.sessionFile={} ".format(REPORT_SESSION) + \
        "-Dconfig.stageFiles.completeFile={} ".format(REPORT_COMPLETE) + \
        "-Dconfig.monthlyReportDate=\\\"{}\\\" ".format(MONTHLY_REPORT) + \
        "-Dconfig.output.excelReportsPath={} ".format(EXCEL_REPORT_PATH) + \
        "-Dconfig.outputDate=$OUTPUT_DATE $LOG ".format(OUTPUT_DATE, log_options)

    ret

def getSparkSubmit(date):
    params = str(getParams(date))

    ret = kinit_command+" && "+ "/usr/bin/spark2-submit --master yarn --queue root.ewhr_technical --deploy-mode client " + \
          "--driver-java-options=\"{}\" --conf spark.executor.extraJavaOptions=\"{}\" --files {}/log4j.properties ".format(params,log_options,WORK_FOLDER) + \
          "--class com.tmobile.sit.ignite.exchangerates.Application {}ignite-1.0-all.jar ".format(WORK_FOLDER)

    ret

spark_submit_template_yesterday =str(getSparkSubmit(DATE_YESTERDAY))

spark_submit_template_today =str(getSparkSubmit(DATE_NOW))


#hdfs dfs -rm $INPUT*

#export PROCESSING_DATE=`date -d"$PROCESSING_DATE - 1 day" "+%Y-%m-%d"`
#hdfs dfs -put /data/input/ewhr/archive/inflight/G_$PROCESSING_DATE* $INPUT
#run_processing


print(spark_submit_template_yesterday)

print(spark_submit_template_today)
