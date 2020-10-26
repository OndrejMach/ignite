package com.tmobile.sit.ignite.deviceatlas.data

import org.apache.spark.sql.{DataFrame, SparkSession}

case class OutputData (terminalDB : DataFrame,
                       d_terminal : DataFrame,
                       d_tac : DataFrame,
                       cptm_vi_d_tac_terminal : DataFrame,
                       cptm_ta_d_terminal_spec : DataFrame)(implicit sparkSession: SparkSession)

