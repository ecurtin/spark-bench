/*
 * (C) Copyright IBM Corp. 2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.sparktc.sparkbench.datagen.mlgenerator

import com.ibm.sparktc.sparkbench.datagen.{DataGenerationConf, DataGenerator}
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object KmeansDataGenDefaults {
  // The parameters for data generation. 100 million points (aka rows) roughly produces 36GB data size
  val NUM_OF_CLUSTERS: Int = 2
  val DIMENSIONS: Int = 20
  val SCALING: Double = 0.6
  val NUM_OF_PARTITIONS: Int = 2

  // Params for workload, in addition to some stuff up there ^^
  val MAX_ITERATION: Int = 2
  val SEED: Long = 127L
}

class KmeansDataGen(conf: DataGenerationConf, sparkSessOpt: Option[SparkSession] = None) extends DataGenerator(conf, sparkSessOpt) {

  import KmeansDataGenDefaults._

  def getOrDefault[A](map: Map[String, Any], name: String, default: A): A = map.get(name) match {
      case Some(x) => x.asInstanceOf[A]
      case None => default
  }

  val m = conf.generatorSpecific //convenience

  val numCluster: Int = getOrDefault(m, "clusters", NUM_OF_CLUSTERS)
  val numDim: Int = conf.numCols
  val scaling: Double = getOrDefault(m, "scaling", SCALING)
  val numPar: Int = getOrDefault(m, "partitions", NUM_OF_PARTITIONS)

  override def generateData(spark: SparkSession): DataFrame = {

    val data: RDD[Array[Double]] = KMeansDataGenerator.generateKMeansRDD(
      spark.sparkContext,
      conf.numRows,
      numCluster,
      numDim,
      scaling,
      numPar
    )

    val schemaString = data.first().indices.map(_.toString).mkString(" ")
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, DoubleType, nullable = false))
    val schema = StructType(fields)

    val rowRDD = data.map(arr => Row(arr:_*))

    spark.createDataFrame(rowRDD, schema)
  }
}