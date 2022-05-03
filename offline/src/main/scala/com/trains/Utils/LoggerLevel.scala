package com.trains.Utils

import org.apache.log4j.{Level, Logger}

trait LoggerLevel {
  Logger.getLogger("org").setLevel(Level.ERROR)
}
