# Overview
This pipeline processes RCSE events and creates outputs for the RCSE dashboards in QS. It is dependent on the
device atlas data (TAC) from where it gets information about types of terminal and firmware version. This pipeline 
is in the terms of data amount the biggest one and that's why it's split to handful of stages. Important fact to notice -
RCSE does 3DES encoding od MSISDNs and IMSIs, but as the amount of data is quite big unique list of these 
identifiers should be extracted and then encoded by the c program included in the cpp folder. This one is 
different from the one in hotspot as it takes a file as an input (file contains a single column with either MSISDNs or MSISDs) encodes them 
and prints the to stdout -> therefore it's needed to re-direct it's output for a file which is later used as a lookup for actual data encryption.
# 3DES encoding
As mentioned earlier to get IMSIs and MSISDNs encoded needed by the processing you need to extract a unique list of them. So you'll have two files - one for MSISDNs and one for IMSIs:
> cat \<inputs\> | cut -f \<position in a file - column #\> -d '|'| sort -u >> temp_file

Then encode them using the C program in the module's cpp folder;
build it using gcc for your platform - creates _a.out_ executable
>gcc encode_msisdn.cpp

then encode the files and forward the output to the resulting file which must be put to the folder read by the application (on prod it's HDFS)
>a.out \<your msisdn or imsi list file\> > msisdn_encoded.csv
# Build
This module is included as a part of the whole ignite project fat jar created by the ShadowJar gradle plugin.
For testing it's not an issue to run it locally using IDEA run capabilities. Dependencies are common and taken from the ignite-common module.
# Configuration
Configuration file can be found in the resources folder, file _rcse.conf_. You can either set the parameters there or 
modify them using JVM parameters setting when launching spark-submit.
List of the parameters follows:

| Parameter | Description | Example |
|-----------|-------------|---------|
|processingDate| Date for which we have data to process | "20201006"|
|inputFilesPath| Where to find input files on HDFS | "/data/sit/rcse/input" |
|maxDate| This date represents actually valid data in the valid_to columns - please dont change as consistency may be harmed.| "47121231"|
|master| cluster manager to use - yarn on cluster, local\[\*\] locally| "Local\[\*\]"|
|dynamicAllocation| sparks dynamic allocation parameter - add resources when needed can be true or false| "true"|
|stageFiles.stagePath| HDFS path where stage data can be found only the important ones are listed below. Most of the data is stored in the parquet format.|"/data/sit/rcse/stage"|
|stageFiles.imsisEncodedPath| files containing encoded IMSIs - created by the 3DES program mentioned above. File shall contain two columns with IMSI and it's encoded version| "/data/sit/rcse/input/imsi_encoded.csv"|
|stageFiles.msisdnsEncodedPath | the same as above, but for MSISDNs||
|stageFiles.tacPath | path to the DeviceAtlas stage file containing Tac codes and terminals|"/data/sit/deviceAtlas/tac.parquet" |
|outputs.outputPath| HDFS path where outputs shall be stored - this files are exported to local filesystem and forwarded to the QS server|"/data/sit/rcse/output"|
|outputs...other params..| used for path to each particular output| |
# Running the app
The prerequisites for running the app is to have configuration properly set (either in the config file or on the commandline when spark-submitting)
and have MSISDNs and IMSIs encoded some files readable by the app. Once you have it you can just run the application in a local mode
using IDEA's run capability or using spark-submit on the cluster. As mentioned application is split into couple of regimes and their order is important.
processing regime is set via commandline parameters:
* *terminalD* - the first stage when inputs are read and new RCSE terminals are extracted to a lookup file
* *events* - the biggest one, handling input RCSE events
* *conf* - getting conf events - it needs today's events to be processed and yesterday's conf file to be available
* *activeUsers* - creates active users files and needs events for today to be processed, conf to be available and also Active users from yesterday + yesterday's events.
* *aggregates* - creates basically the outputs from the files above and stores them in the stage folder. Some files have dependency on their yesterday's version.
* *output* - extracts today's outputs from the stage folder and data created by the phases above.

*logs* - logging configuration is passed to all the nodes via _--files_ switch together with the _log4j.configuration_ JVM parameter

Spark-submit example for the *terminalD* phase:
>spark-submit --master yarn --queue root.ewhr_technical --deploy-mode client\
>--driver-java-options="-Dconfig.processingDate=20201130\
>-Dconfig.inputFilesPath=/data/sit/rcse/input/TMD_*\
>-Dconfig.master=yarn\
>-Dconfig.stageFiles.stagePath=/data/sit/rcse/stage/\
>-Dconfig.stageFiles.tacPath=/data/sit/deviceatlas/stage/cptm_ta_d_tac.parquet/\
>-Dconfig.stageFiles.imsisEncodedPath=/data/sit/rcse/input/imsi_encoded.csv\
>-Dconfig.stageFiles.msisdnsEncodedPath=/data/sit/rcse/input/msisdn_encoded.csv\
>-Dconfig.outputs.outputPath=/data/sit/rcse/out/\
>-Dlog4j.configuration=file:/data/jobs/sit/rcse/log4j.properties"\
>--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:/data/jobs/sit/rcse/log4j.properties"\
>--files /data/jobs/sit/rcse//log4j.properties\
>--class com.tmobile.sit.ignite.rcse.Application\
>/data/jobs/sit/rcse/ignite-1.0-all.jar terminalD