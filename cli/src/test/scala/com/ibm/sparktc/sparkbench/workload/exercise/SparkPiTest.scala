package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class SparkPiTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  val spark = SparkSessionProvider.spark

  "SparkPi" should "instantiate and run" in {
    val workload = SparkPi(Map("slices" -> 4))
    val res = workload.doWorkload(None, spark).collect
    res.length shouldBe 1
    val row = res(0)
    row.length shouldBe 4
    row.getAs[String]("name") shouldBe "sparkpi"
    row.getAs[Double]("pi_approximate") shouldBe 3.14 +- 1
  }

  "SparkPi" should "do a dry run properly" in {
    val res = SparkPi(Map("slices" -> 4)).run(spark, true).collect
    res.length shouldBe 1
    val cols = res(0).schema.toList.map(_.name)
    cols.length shouldBe 3
    cols should not contain "pi_approximate"
  }
}
