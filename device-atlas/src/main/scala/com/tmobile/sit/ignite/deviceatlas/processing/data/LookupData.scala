package com.tmobile.sit.ignite.deviceatlas.processing.data
import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.ignite.deviceatlas.datastructures.FileStructures
import org.apache.spark.sql.SparkSession

class LookupData(lookupPath : String)(implicit sparkSession : SparkSession) extends Logger {

  val manufacturer = {
    val file = lookupPath + "manufacturer.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.manufacturer_Lkp),
      delimiter = "|")
      .read()
  }

  val manufacturerVendor = {
    val file = lookupPath + "manufacturer_vendor.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.manufacturerVendor_Lkp),
      delimiter = "|")
      .read()
  }

  val operatingSystem = {
    val file = lookupPath + "operating_system.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.operatingSystem_Lkp),
      delimiter = "|")
      .read()
  }

  val osNokia = {
    val file = lookupPath + "operating_system_nokia_os.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.osNokia_Lkp),
      delimiter = "|")
      .read()
  }

  val terminalDB = {
    val file = lookupPath + "terminaldb.csv" // TODO: check correct path
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.terminalDB_full_lkp),
      delimiter = "|")
      .read()
  }

  val tacBlacklist = {
    val file = lookupPath + "tac_blacklist.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.tacBlacklist_Lkp),
      delimiter = "|")
      .read()
  }

  val terminalId = {
    val file = lookupPath + "terminadb_terminal_id.hwm"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.terminal_id_lkp),
      delimiter = "|",
      encoding = "CP1250")
      .read()
  }

  val historical_terminalDB = {
    val file = lookupPath + "terminal_database_export.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.terminalDB_full_lkp),
      delimiter = "|",
      encoding = "CP1250"
    )
      .read()
  }

  val extra_terminalDB_records = {
    val file = lookupPath + "extra_in_terminaldb.csv"
    logger.info(s"Reading file: ${file}")
    CSVReader(file,
      header = false,
      schema = Some(FileStructures.terminalDB_full_lkp),
      delimiter = "|")
      .read()
  }

}
