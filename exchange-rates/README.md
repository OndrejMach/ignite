# Overview
Exchange rates pipeline processes actual rates for currency conversion and prepares them for processing in Inflight and Hotspot ppelines.
The idea is to have all the money transfers in EUR -> processing reflects that. What it does as well is historising of the outdated rates
for potential analysis. Output should stored on HDFS in some common folder.
## Configuration
Main configuration is in the resources folder, file _exchange_rates.conf_. The following parameters can be changed:
* *processing_date* - for what date processing is executed
* *input_date* - date in the input filename
* *master* - what cluster manager to use. 'local\[\*\]' runs it on local machine, 'yarn' will use the yarn scheduler.
* *input* - here you can specify from where to read the file. Folder and filename is separated to minimise risk of error as input filename follows some strict rules - better to specify folder and keep the filename as is.
* *stage* - where to store the result. Again as for the input better to set only stage folder and keep the filename as is to minimise risk of typos. File is written in the parquet format.
## Running it
Execution is very straight-forward, just running it using spark-submit with setting Exchange-rates main class.. as follows:
*Local regime - development and testing: * Just run it in IntelliJ or via gradle - don't forget to change the parameters accordingly.
*Cluster* - using spark-submit; keep in mind pipeline is delivered together with all the module as a fat jar. Example below also shows how to propagate log configuration file.

*logs* - logging configuration is passed to all the nodes via _--files_ switch together with the _log4j.configuration_ JVM parameter

> /opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client\
> --driver-java-options="-Dconfig.input_date=20201130\
> -Dconfig.input.input_folder=/data/sit/exchange_rates/input/\
> -Dconfig.master=yarn\
> -Dconfig.stage.exchange_rates_filename=/data/sit/common/cptm_ta_t_exchange_rates\
> -Dconfig.stage.stage_folder=/data/sit/common/\
> -Dlog4j.configuration=file:/data/jobs/sit/exchange_rates_spark_app/log4j.properties"\
> --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:/data/jobs/sit/exchange_rates_spark_app/log4j.properties"\
> --files /data/jobs/sit/exchange_rates_spark_app//log4j.properties\
> --class com.tmobile.sit.ignite.exchangerates.Application /data/jobs/sit/exchange_rates_spark_app/ignite-1.0-all.jar