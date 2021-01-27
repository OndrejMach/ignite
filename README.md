# The Ignite project
## Overview
This project implements the following ETL pipelines written in Spark/Scala:
* Inflight
* RCSE
* Hotspot
* Exchange rates
* Device Atlas
* RCSEU

Each pipeline is implemented in the separate module. There are two exceptions; 
* ignite-common - contain common code useful for more modules
* job-template - this one is some kind of inspiration when there is a need to create a new pipeline (module)

Generally all the pipelines follow the same principles and coding/building standards. You can find detailed documentation 
in an appropriate module.
Good point to stress here is that project is normally deployed as a big Jar containing all the dependencies and 
modules - build is done using ShadowJar Gradle plugin. Execution of each particular pipeline can be selected when spark-submitting by selecting
appropriate main class. 
## Build
Project is build using Gradle with ShadowJar plugin. ShadowJar downloads all teh dependencies because
cluster is isolated from the internet and any external libraries can't be downloaded (typesafe config, library for excel read/write).

You can build the whole project by executing the *shadowJar* target.

Important note here is that dependencies are not downloaded directly from the common Maven 
sources, but via maven proxy on CDCP workbench's nexus.

Configuration on what to build can be found in _settings.gradle_ file. As you can see job-template is not listed there and therefore not
put in the target jar file.

## Dependecies
Details about dependencies can be found in the _build.gradle_ file. To get all working you need access to the CDCP workbench and especially to the nexus3 repository.
In order to enable the automatic build you need to set-up your _gradle.properties_ in your home's _.gradle_ file. See the CDCP workbench documentation for details.

## Coding guidelines
When you start contributing to the project there are couple of habits you should follow:
* Every change shall be implemented in a separate branch - no changes directly on the master branch
* Push to master only via merge request approved by your fellow developers
* If possible include tests

## Deployment/Run on production
Project jar is deployed on the CDR store's edge node and pipelines are then run via the airflow orchestrator.

 