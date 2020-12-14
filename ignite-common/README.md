# Common Libs
## Overview
This module contains code which is re-used in more then one module. Also it contains the master build.gradle file with aligned dependencies.
All the pipeline modules include gradle dependencies from this place to keep important libs and their versions in sync. In case you do any major 
update of spark or typesafe config please do it here.

## Common functionality
* There is a structure of exchange rates which are used in Inflight and Hotspot
* Implicit dataframe manipulation - implicit function to get column names in capital letters - very common as outputs expect this.
* Processing function for reading and joining with exchange rates. So basically how to get correct currency conversion properly.
* Readers for Exchange rates and Tac codes - helps to get the correct version of the data you need.
* Logging configuration - in the _resources_ folder you can find the main log config file


## Dependencies
As already mentioned this module contain all the common dependencies. This is beneficial especially from the consistency perspective which means
in case you'd like to upgrade a specific library you can do it here and your change is automatically propagated to all the important modules. 
The important ones are:
* spark
* scala
* crealytics's spark readers/writers used in spark_common
* typesafe for configuration handling
* ucanaccess for MS exchange files handling (+ related libs hsql and jackcess)
 