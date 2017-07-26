package com.ibm.sparktc.sparkbench.workload.exercise

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SparkFuncs
import org.apache.spark.sql.{DataFrame, SparkSession}

object Sleep extends WorkloadDefaults {
  val name = "sleep"
  def apply(m: Map[String, Any]) =
    new Sleep(input = m.get("input").map(_.asInstanceOf[String]),
      output = None,
      sleepMS = (m.get("sleepms"), m.get("maxsleepms")) match {
        case (Some(l), _) => any2Int2Long(l)
        case (None, Some(l)) => randomLong(max = any2Int2Long(l))
        case (_, _) => randomLong(max = 3600000L) //one hour
      }
    )
}

case class Sleep(
                input: Option[String] = None,
                output: Option[String] = None,
                sleepMS: Long
              ) extends Workload {

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    Thread.sleep(sleepMS)
    SparkFuncs.zeroColRes(spark)
  }

}

