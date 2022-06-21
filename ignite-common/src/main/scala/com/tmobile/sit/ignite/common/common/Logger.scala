package com.tmobile.sit.ignite.common.common

import org.slf4j.LoggerFactory


/**
 * Logger class providing slf4j based logger. Logger configuration can be provided in log4j.properties (for example in the resources folder).
 *
 */
trait Logger {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
