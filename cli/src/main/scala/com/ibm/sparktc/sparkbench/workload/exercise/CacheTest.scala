package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CacheTestResult(runIndex: Int,timestamp1: Long, runTime: Long)

object CacheTest extends WorkloadDefaults {
  val name = "cachetest"

  def apply(m: Map[String, Any]) =
    CacheTest(input = m.get("input").map(_.asInstanceOf[String]),
      output = m.get("workloadresultsoutputdir").map(_.asInstanceOf[String]),
      delayMs = getOrDefault(m, "delayMs", 0L),
      sleepMs = getOrDefault(m, "sleepMs", 1000L))
}

case class CacheTest(input: Option[String],
                     output: Option[String],
                     delayMs: Long,
                     sleepMs: Long) extends Workload {


  override def run(spark: SparkSession): DataFrame = {
    Thread.sleep(delayMs)
    super.run(spark)
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val cached = df.getOrElse(Seq.empty[(Int)].toDF).cache

    val timestamp1 = System.currentTimeMillis()
    val (resultTime1, _) = time(cached.count)
    Thread.sleep(sleepMs)
    val timestamp2 = System.currentTimeMillis()
    val (resultTime2, _) = time(cached.count)
    Thread.sleep(sleepMs)
    val timestamp3 = System.currentTimeMillis()
    val (resultTime3, _) = time(cached.count)

    val results = Seq(CacheTestResult(1, timestamp1, resultTime1),
      CacheTestResult(2, timestamp2, resultTime2),
      CacheTestResult(3, timestamp3, resultTime3))

    cached.unpersist(blocking = true)
    
    spark.createDataFrame(results)
  }
}
