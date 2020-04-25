package com.tmobile.sit.ignite.hotspot.config

case class OrderDBConfig(orderDBFile: Option[String], wlanHotspotFile: Option[String], errorCodesFile: Option[String])

case class Settings (
                    orderDBFiles: OrderDBConfig
                    )
