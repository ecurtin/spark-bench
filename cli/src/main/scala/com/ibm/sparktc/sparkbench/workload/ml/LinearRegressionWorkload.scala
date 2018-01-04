/**
  * (C) Copyright IBM Corp. 2015 - 2018
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package com.ibm.sparktc.sparkbench.workload.ml

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.{getOrDefault, getOrThrow, time}
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.ibm.sparktc.sparkbench.utils.SparkFuncs._
import org.apache.spark.ml.regression.LinearRegression

case class LinearRegressionResult(
                                     name: String,
                                     appid: String,
                                     start_time: Long,
                                     input: String,
                                     train_count: Long,
                                     train_time: Long,
                                     test_file: String,
                                     test_count: Long,
                                     test_time: Long,
                                     load_time: Long,
                                     count_time: Long,
                                     save_time: Long,
                                     total_runtime: Long,
                                     meanSquaredError: Double
                                   )

object LinearRegressionWorkload extends WorkloadDefaults {

  val name = "linear-regression"
  def apply(m: Map[String, Any]): LinearRegressionWorkload =
    LinearRegressionWorkload(
      input = Some(getOrThrow(m, "input").asInstanceOf[String]),
      output = getOrDefault[Option[String]](m, "workloadresultsoutputdir", None),
      testFile = getOrThrow(m, "testfile").asInstanceOf[String],
      maxIterations = getOrDefault[Int](m, "max-iterations", 3),
      numPartitions = getOrDefault[Int](m, "numpartitions", 32),
      cacheEnabled = getOrDefault[Boolean](m, "cacheenabled", true)
    )
}

case class LinearRegressionWorkload(
                                     input: Option[String],
                                     output: Option[String],
                                     testFile: String,
                                     maxIterations: Int,
                                     numPartitions: Int,
                                     cacheEnabled: Boolean
                                   ) extends Workload {

  private[ml] def loadAndCache(fn: String)(implicit spark: SparkSession) = time {
    val ds = load(spark, fn).repartition(numPartitions)
    if (cacheEnabled) ds.cache
    ds
  }

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    val startTime = System.currentTimeMillis
    val (ltrainTime, trainingData) = loadAndCache(s"${input.get}")(spark)
    val (ltestTime, testData) = loadAndCache(s"$testFile")(spark)
    val (countTime, (trainCount, testCount)) = time { (trainingData.count(), testData.count()) }
    val (trainTime, model) = time(new LinearRegression().setMaxIter(maxIterations).fit(trainingData))
    val (testTime, linearRegressionSummary) = time(model.evaluate(testData))
    val (savetime, _) = output match {
      case Some(dir) => time(writeToDisk(dir, linearRegressionSummary.predictions, spark))
      case _ => (0L, Unit)
    }

    val loadTime = ltrainTime + ltestTime

    spark.createDataFrame(
      Seq(
        LinearRegressionResult(
          name = LinearRegressionWorkload.name,
          appid = spark.sparkContext.applicationId,
          start_time = startTime,
          input = input.get,
          train_count = trainCount,
          train_time = trainTime,
          test_file = testFile,
          test_count = testCount,
          test_time = testTime,
          load_time = loadTime,
          count_time = countTime,
          save_time = savetime,
          total_runtime = loadTime + trainTime + testTime,
          meanSquaredError = linearRegressionSummary.meanSquaredError
        ))
    )
  }

  // TODO allow training datasets to be generated from test datasets
  // TODO see if other info in linearRegressionSummary can be output easily, look into how results are curried with spark info
  // TODO tests :(
}