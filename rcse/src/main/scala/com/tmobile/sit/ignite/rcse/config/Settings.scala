package com.tmobile.sit.ignite.rcse.config

import java.sql.Date

case class Settings (
                      terminalPath: String,
                      tacPath: String,
                      maxDate: Date,
                      outputPath: String
                    )