package com.ibm.sparktc.sparkbench.workload

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.ibm.sparktc.sparkbench.utils.SparkFuncs.{load, addConfToResults}
import org.apache.spark.sql.types._
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.time
import com.ibm.sparktc.sparkbench.utils.SparkFuncs

trait WorkloadDefaults {
  val name: String
  def apply(m: Map[String, Any]): Workload
}

trait Workload {
  val input: Option[String]
  val output: Option[String]

  /**
    *  Validate that the data set has a correct schema and fix if necessary.
    *  This is to solve issues such as the KMeans load-from-disk pathway returning
    *  a DataFrame with all the rows as StringType instead of DoubleType.
   */
  def reconcileSchema(dataFrame: DataFrame): DataFrame = dataFrame

  /**
    * Actually run the workload.  Takes an optional DataFrame as input if the user
    * supplies an inputDir, and returns the generated results DataFrame.
    */
  def doWorkload(df: Option[DataFrame], sparkSession: SparkSession): DataFrame

  def run(spark: SparkSession, dryRun: Boolean = false): DataFrame = {
    val start = System.currentTimeMillis
    val (runtime, res) = if (dryRun) {
      (0L, SparkFuncs.zeroColRes(spark))
    }
    else {
      val df = input match {
        case None => None
        case Some(in) => Some(reconcileSchema(load(spark, in)))
      }
      time(doWorkload(df, spark).coalesce(1))
    }
    composeResults(spark, res, start, runtime)
  }

  def composeResults(spark: SparkSession, df: DataFrame, start: Long, runtime: Long): DataFrame = {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("start_time", LongType),
      StructField("total_runtime", LongType)
    ))
    val info = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row("name", start, runtime))), schema)
    addConfToResults(info.crossJoin(df), toMap)
  }

  def toMap: Map[String, Any] =
    (Map[String, Any]() /: this.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(this))
    }
}
