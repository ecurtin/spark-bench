package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class CachingResult(runIndex: Int, timestamp1: Long, runTime: Long)

/**
 * This workload is meant to test sparks use of cached dataframes. In this workload we time how long
 * it takes to actualize a dataframe and cache it. Then we pause, then we time how long it takes to
 * perform the same action on the cached dataframe again.
 */
object Caching extends WorkloadDefaults {
  val name = "cachetest"

  def apply(m: Map[String, Any]) = Caching(
    input = m.get("input").map(_.asInstanceOf[String]),
    output = m.get("workloadresultsoutputdir").map(_.asInstanceOf[String]),
    delayMs = m("delayms").asInstanceOf[Int],
    sleepMs = m("sleepms").asInstanceOf[Int],
    mapDelayMicros = m("mapdelaymicros").asInstanceOf[Int])
}

case class Caching(input: Option[String],
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

    logger.debug(s"the cache timeout is ${spark.conf.get("spark.dynamicAllocation.cachedExecutorIdleTimeout", "not set")}")
    logger.debug(s"start sleeping for $sleepMs millis at ${System.currentTimeMillis()}")
    Thread.sleep(sleepMs)
    logger.debug(s"stop sleeping fo $sleepMs at ${System.currentTimeMillis()}")

    val timestamp2 = System.currentTimeMillis()
    val (resultTime2, _) = time(runCalc(cached))

    val results = Seq(CachingResult(1, timestamp1, resultTime1),
      CachingResult(2, timestamp2, resultTime2))

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
