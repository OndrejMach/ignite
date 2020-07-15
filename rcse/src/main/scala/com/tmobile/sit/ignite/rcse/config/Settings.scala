package com.tmobile.sit.ignite.rcse.config

import java.sql.Date

case class Settings (
                      inputFilesPath: String,
                      clientPath: String,
                      terminalSWPath: String,
                      imsisEncodedPath: String,
                      msisdnsEncodedPath: String,
                      terminalPath: String,
                      tacPath: String,
                      maxDate: Date,
                      outputPath: String,
                      encoderPath: String,
                      regDerEventsToday: String,
                      regDerEventsYesterday: String,
                      activeUsersToday: String,
                      activeUsersYesterday: String,
                      confFile: String,
                      initUser: String,
                      initConf: String
                    )