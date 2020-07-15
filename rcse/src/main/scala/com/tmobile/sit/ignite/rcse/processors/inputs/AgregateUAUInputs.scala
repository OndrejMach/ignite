package com.tmobile.sit.ignite.rcse.processors.inputs

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.rcse.config.Settings
import com.tmobile.sit.ignite.rcse.structures.ActiveUsers
import org.apache.spark.sql.SparkSession

class AgregateUAUInputs(settings: Settings)(implicit sparkSession: SparkSession) extends Logger {
  val activeUsersData = CSVReader(
    path = settings.activeUsersToday,//"/Users/ondrejmachacek/Projects/TMobile/EWH/EWH/rcse/data/stage/cptm_ta_f_rcse_active_user.TMD.20200607.csv",
    header = false,
    delimiter = "|",
    schema = Some(ActiveUsers.activeUsersSchema)
  )
    .read()
    .drop("entry_id", "load_date")
}
