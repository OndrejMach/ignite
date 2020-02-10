package com.tmobile.sit.ignite.common.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Properties

class ServiceConfig(filename: Option[String] = None) {
  val ARRAY_DELIMITER: String = ","

  val config: Config = filename.fold(ifEmpty = ConfigFactory.load())(file => ConfigFactory.load(file))

  def envOrElseConfig(name: String): String = {
    config.resolve()
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }

  def getInt(name: String): Option[Int] = {
    try {
      val i = envOrElseConfig(name).toInt
      if (i>=0) {
        Some(i)
      } else {
        None
      }
    } catch { case e : Exception => None}
  }

  def getLong(name: String) : Option[Long] = {
    try {
      Some(envOrElseConfig(name).toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def getDouble(name: String) : Option[Double] = {
    try {
      Some(envOrElseConfig(name).toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def getString(name: String) : Option[String] = {
    try {
      Some(envOrElseConfig(name))
    } catch {
      case e: Exception => None
    }
  }

  def getIntArray(name: String) : Option[Array[Int]] = {
    try {
      val s = Some(envOrElseConfig(name)).get
      Some(s.split(ARRAY_DELIMITER).map(_.toInt))
    } catch {
      case e: Exception => None
    }
  }
}