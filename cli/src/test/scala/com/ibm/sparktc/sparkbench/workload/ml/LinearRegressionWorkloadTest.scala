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

import java.io.File

import com.holdenkarau.spark.testing.Utils
import com.ibm.sparktc.sparkbench.datageneration.mlgenerator.LinearRegressionDataGen
import com.ibm.sparktc.sparkbench.testfixtures.SparkSessionProvider
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class LinearRegressionWorkloadTest  extends FlatSpec with Matchers with BeforeAndAfterEach {
  val spark = SparkSessionProvider.spark
  val fileName = s"/tmp/spark-bench-scalatest/linear-reg-${java.util.UUID.randomUUID.toString}.parquet"

  override def afterEach() {
    Utils.deleteRecursively(new File(fileName))
  }

  def makeDataFrame(): DataFrame = {
    val m = Map(
      "rows" -> 100,
      "cols" -> 10,
      "output" -> fileName
    )

    LinearRegressionDataGen(m).doWorkload(None, spark)
  }

  "blah" should "stuff" in {
    val df = makeDataFrame()
    val conf = Map("name" -> "linear-regression", "input" -> fileName, "testfile" -> fileName)
    val work = LinearRegressionWorkload(conf)
    val ddf = work.doWorkload(Some(df), spark)
    ddf.show()
  }

}
