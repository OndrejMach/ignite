package com.tmobile.sit.ignite.deviceatlas.processing.data

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

case class OutputData (terminalDB : DataFrame,
                       d_terminal : DataFrame,
                       d_tac : DataFrame,
                       cptm_vi_d_tac_terminal : DataFrame,
                       cptm_ta_d_terminal_spec : DataFrame)(implicit sparkSession: SparkSession) extends Logger{

    def write(path: String, ODATE: String) = {
        logger.info(s"Writing ${path}terminaldb_$ODATE.csv")
        CSVWriter(data = terminalDB,
            path = s"${path}terminaldb_$ODATE.csv",  delimiter = "|", writeHeader = false, escape = "", quote = "", encoding = "CP1250").writeData()

        logger.info(s"Writing ${path}cptm_ta_d_terminal_spec.tmp")
        CSVWriter(data = d_terminal,
            path = s"${path}cptm_ta_d_terminal_spec.tmp",  delimiter = "|", writeHeader = false, escape = "", quote = "").writeData()

        logger.info(s"Writing ${path}cptm_ta_d_tac.tmp")
        CSVWriter(data = d_tac,
            path = s"${path}cptm_ta_d_tac.tmp",  delimiter = "|", writeHeader = false, escape = "", quote = "").writeData()

        logger.info(s"Writing ${path}cptm_vi_d_tac_terminal_${ODATE}.csv")
        CSVWriter(data = cptm_vi_d_tac_terminal,
            path = s"${path}cptm_vi_d_tac_terminal_${ODATE}.csv",  delimiter = ";", writeHeader = false).writeData()

        logger.info(s"Writing ${path}cptm_ta_d_terminal_spec_${ODATE}.csv")
        CSVWriter(data = cptm_ta_d_terminal_spec,
            path = s"${path}cptm_ta_d_terminal_spec_${ODATE}.csv",  delimiter = ";", writeHeader = false, escape = "", quote = "", encoding = "CP1250").writeData()

    }

}

