package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.cli.Configurator
import com.ibm.sparktc.sparkbench.workload.{ConfigCreator, MultiSuiteRunConfig, Suite}
import java.io.File
import org.scalatest.{FlatSpec, Matchers}

class CachingTest extends FlatSpec with Matchers {
  "Caching workload" should "be read from conf correctly" in {
    val url = getClass.getResource("/cachetest.conf")
    val file = new File(url.toURI)

    val result: MultiSuiteRunConfig = Configurator(file).head
    val suite: Suite = result.suites.head
    val workloadConfig = suite.workloadConfigs.head
    val workload = ConfigCreator.mapToConf(workloadConfig)

    workload shouldBe a [Caching]
    val cacheTest = workload.asInstanceOf[Caching]
    cacheTest.sleepMs shouldBe 10000
    cacheTest.delayMs shouldBe 0
    cacheTest.input shouldBe Some("input")
    cacheTest.mapDelayMicros shouldBe 10
  }

}
