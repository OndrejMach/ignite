package com.tmobile.sit.ignite.common

import org.slf4j.LoggerFactory

trait Logger {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
