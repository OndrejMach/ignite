# Overview
Hotspot pipeline provides data for dashboards reflecting activity around WLAN hotspots. It gathers data about traffic, each particular hotspot's info (location, name etc.) and payment transactions.
Data is gathered via the EFB system from the CSA component. Apart from the data for dashboards it creates also information about WLAN vouchers and payment transactions. Processing requires regular 
daily input as well as Exchange rates data. There is also couple of static tables which are used as lookups. From the architecture perspective see the SIT team's confluence pages.
# 3DES encoding
Very special thing here is that processing calls an external routing to encrypt sensitive information. It's executed as a Spark UDF. The source code can be found in the *3DES-Cpp* folder -
to make it running just compile it using gcc on your desired platform. 
>gcc encode_msisdn.cpp

Creates executable file _a.out_ - path to this file must be specified in the job's configuration. 
# Build
Hotspot is build together with all the other pipelines in one big jar also containing all the dependencies using the Gradle's shadowJar plugin. Whe you are starting it, just specify the main 
class of the Hotspot module which is _com.tmobile.sit.ignite.hotspot.Application_.
# Configuration
All to configuration parameters are stored in the _resources_ folder, file _hotspot.conf_. The list of parameters follow:

| *Parameter* | *Description* | *Example* |
|-----------|-------------|---------|
|processing_date | Data for what date should be processed | "20201011" |
|input_date| Input file date to process (normally it's processing_date - 1day) | "20201012"|
|3DES_encoder_path| path to the 3DES encoding routine - It's called from a spark UDF| "3DES-Cpp/a.out"|
|input.input_folder| path where input files are available (taken only the ones having input_date in the name)| "/data/input"|
|input.*filename| filenames of the each particular input file| "${config.input.input_folder}"TMO.FAILEDLOGINS.DAY.*.csv"|
|stage.stage_folder| path to the stage folder on HDFS - all the stage data should be placed there| "/data/sit/hotspot/stage"|
|stage..other parms..| Paths to each particular stage data. Either interim or outputs|...|
|output.output_folder| Path where all the output files are stored on HDFS|"/data/sit/hotspot/output"|
|output..other params..| File name for each particular processing output. Files are delivered to the QS server.|...|
# Running the app
As said application is built together with all the other pipelines in the ignite project in one single fat jar. Of course for testing you can create your own build
task in gradle or use your IDE running capabilities. The production mode is available only using this big jar. To run hotspot you need to specify all the parameters
according to your environment and then run it using spark-submit command. Parameters can be also overwritten in the spark-submit command by specifying JVM params.
Example can be found below. Important thing to note here is that application is executed in 4 regimes specified as a commandline parameters - the order here is important for a new data:
*  *input* - processes input files and generates mapVoucher, OrderDB, wlan hotspot, and CDR files (the first two are used in the inflight pipeline) - this regime writes to
the stage folder. Important thing here is that it appends so in case you run it twice for the same date, you get duplicates.
* *stage* - processes stage structures created in the input regime and creates output data in the stage folder.
* *output* - extracts output data from the stage folder to the format expected by the target dashboards.
* *wina_reports* - generates data for export towards the oracle WINA database (as CSVs)

*logs* - logging configuration is passed to all the nodes via _--files_ switch together with the _log4j.configuration_ JVM parameter

Running it on the cluster:

>spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client 
>--driver-java-options="-Dconfig.processing_date=20201130\
>-Dconfig.input_date=20201201\ 
>-Dconfig.3DES_encoder_path=./a.out\
>-Dconfig.input.input_folder=/data/sit/hotspot/input/\
>-Dconfig.master=yarn\
>-Dconfig.stage.stage_folder=/data/sit/hotspot/stage/\
>-Dconfig.stage.exchange_rates_filename=/data/sit/common/cptm_ta_t_exchange_rates\
>-Dconfig.output.output_folder=/data/sit/hotspot/out/\
>-Dlog4j.configuration=file:/data/jobs/sit/hotspot/log4j.properties"\
>--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:/data/jobs/sit/hotspot/log4j.properties"\ 
>--files /data/jobs/sit/hotspot//log4j.properties,/data/jobs/sit/hotspot/a.out\
> --class com.tmobile.sit.ignite.hotspot.Application /data/jobs/sit/hotspot/ignite-1.0-all.jar input