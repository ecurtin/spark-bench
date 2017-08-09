package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class CacheTestResult(runIndex: Int,timestamp1: Long, runTime: Long)

object CacheTest extends WorkloadDefaults {
  val name = "cachetest"

  def apply(m: Map[String, Any]) = CacheTest(
    input = m.get("input").map(_.asInstanceOf[String]),
    output = m.get("workloadresultsoutputdir").map(_.asInstanceOf[String]),
    delayMs = getOrDefault(m, "delayMs", 0L),
    sleepMs = getOrDefault(m, "sleepMs", 1000L),
    mapDelayMicros = getOrDefault(m, "mapDelayMicros", 1L))
}

case class CacheTest(input: Option[String],
                     output: Option[String],
                     delayMs: Long,
                     sleepMs: Long,
                     mapDelayMicros: Long
                    ) extends Workload {

  @transient val logger = LoggerFactory.getLogger(getClass)

  override def run(spark: SparkSession): DataFrame = {
    Thread.sleep(delayMs)
    super.run(spark)
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val cached = df.getOrElse(Seq.empty[(Long)].toDF)
      .map(row => row.getInt(0).toLong) //TODO bk getInt here?
      .rdd
      .map { x =>
        busyWait(mapDelayMicros * 1000)
        x + 1
      }
      .cache

    val timestamp1 = System.currentTimeMillis()
    val (resultTime1, _) = time(runCalc(cached))
    logger.debug("start sleeping")
    Thread.sleep(sleepMs)
    logger.debug("stop sleeping")
    val timestamp2 = System.currentTimeMillis()
    val (resultTime2, _) = time(runCalc(cached))

    val results = Seq(CacheTestResult(1, timestamp1, resultTime1),
      CacheTestResult(2, timestamp2, resultTime2))

    cached.unpersist(blocking = true)
    
    spark.createDataFrame(results)
  }

  def runCalc(rdd: RDD[Long]): Long = rdd.reduce((x, y) => x + y)

  //according to stack overflow this is the best way to wait for sub millisecond perios
  //Tested on my machine and is accurate to around +- 200 nanos when you wait for a microsecond
  private def busyWait(nanos: Long): Unit = {
    val start = System.nanoTime
    while(System.nanoTime - start < nanos) {
      //do nothing
    }
  }
}
