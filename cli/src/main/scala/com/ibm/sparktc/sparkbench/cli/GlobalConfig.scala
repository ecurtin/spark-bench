package com.ibm.sparktc.sparkbench.cli

import com.typesafe.config.Config

import scala.util.Try

object GlobalConfig {
  var dryRun: Boolean = false

  def fromConf(config: Config): Unit = {
    Try(config.getBoolean("dry-run")).foreach(dryRun = _)
  }
}
