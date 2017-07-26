package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.{getOrDefault, time}
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.random

case class SparkPiResult(pi_approximate: Double)

object SparkPi extends WorkloadDefaults {
  val name = "sparkpi"
  def apply(m: Map[String, Any]) =
    new SparkPi(input = m.get("input").map(_.asInstanceOf[String]),
      output = None,
      slices = getOrDefault[Int](m, "slices", 2)
    )
}

case class SparkPi(input: Option[String] = None,
                    output: Option[String] = None,
                    slices: Int
                  ) extends Workload {

  // Taken directly from Spark Examples:
  // https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
  private def sparkPi(spark: SparkSession): Double = {
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if ((x * x) + (y * y) <= 1) 1 else 0
    }.reduce(_ + _)
    val piApproximate = 4.0 * count / (n - 1)
    piApproximate
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    spark.createDataFrame(Seq(SparkPiResult(sparkPi(spark))))
  }

}
