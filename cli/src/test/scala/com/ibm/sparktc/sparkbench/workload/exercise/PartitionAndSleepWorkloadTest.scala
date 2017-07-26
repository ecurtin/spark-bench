package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import org.scalatest.{FlatSpec, Matchers}

class PartitionAndSleepWorkloadTest extends FlatSpec with Matchers {
  val spark = SparkSessionProvider.spark

  "PartitionAndSleepWorkload" should "instantiate and run" in {
    val workload = PartitionAndSleepWorkload(Map("name" -> "timedsleep", "partitions" -> 10, "sleepms" -> 10))
    val res = workload.run(spark).collect
    res.length shouldBe 1
    val row  = res(0)
    row.length shouldBe 7
    //row.getAs[String]("name") shouldBe "name"
    row.getAs[Long]("start_time") shouldBe System.currentTimeMillis +- 10000
  }
}
