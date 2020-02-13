package com.tmobile.sit.ignite.common.config

import com.tmobile.sit.ignite.common.Logger

abstract class GenericSettings extends Logger {
  def printMissingFields(): Unit = {
    val fields = this.getClass.getDeclaredFields
    for (field <- fields) {
      field.setAccessible(true)
      val name = field.getName
      if (field.get(this).asInstanceOf[Option[String]].get.isEmpty)
        logger.info(s"${Console.RED}Missing entry: $name${Console.RESET}")
    }
  }

  def printAllFields(): Unit = {
    val fields = this.getClass.getDeclaredFields
    for (field <- fields) {
      field.setAccessible(true)
      logger.info(s"${Console.RED}${Console.BOLD}Parameter ${field.getName}:${Console.RESET} ${field.get(this).asInstanceOf[Option[String]].get}")
    }
  }

  def isAllDefined: Boolean
}