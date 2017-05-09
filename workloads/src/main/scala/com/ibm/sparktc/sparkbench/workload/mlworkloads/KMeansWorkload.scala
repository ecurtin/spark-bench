package com.ibm.sparktc.sparkbench.workload.mlworkloads

import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadConfig}
import com.ibm.sparktc.sparkbench.utils.SparkFuncs.writeToDisk
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import com.ibm.sparktc.sparkbench.utils.KMeansDefaults
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._

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

case class KMeansWorkloadConfig(
                                 name: String,
                                 inputDir: Option[String],
                                 workloadResultsOutputDir: Option[String],
                                 k: Int,
                                 seed: Long,
                                 maxIterations: Int
                       ) extends WorkloadConfig {

  def this(m: Map[String, Any], spark: SparkSession) = {
    this(
        name = verifyOrThrow(m, "name", "kmeans", s"Required field name does not match"),
        inputDir = Some(getOrThrow(m, "input").asInstanceOf[String]),
        workloadResultsOutputDir = getOrDefault[Option[String]](m, "workloadresultsoutputdir", None),
        k = getOrDefault(m, "k", KMeansDefaults.NUM_OF_CLUSTERS),
        seed = getOrDefault(m, "seed", KMeansDefaults.SEED, Some(any2Int2Long)),
        maxIterations = getOrDefault(m, "maxiterations", KMeansDefaults.MAX_ITERATION)
      )
  }


}


class KMeansWorkload(conf: KMeansWorkloadConfig, spark: SparkSession) extends Workload[KMeansWorkloadConfig](conf, spark){

  override def doWorkload(df: Option[DataFrame], spark: SparkSession): DataFrame = {
    val (loadtime, data) = loadToCache(df.get, spark) // Should fail loudly if df == None
    val (trainTime, model) = train(data, spark)
    val (testTime, _) = test(model, data, spark)
    val (saveTime, _) = conf.workloadResultsOutputDir match {
      case Some(_) => save(data, model, spark)
      case _ => (null, Unit)
    }

    val schema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("load", LongType, nullable = true),
        StructField("train", LongType, nullable = true),
        StructField("test", LongType, nullable = true),
        StructField("save", LongType, nullable = true)
      )
    )

    val timeList = spark.sparkContext.parallelize(Seq(Row("kmeans", System.currentTimeMillis(), loadtime, trainTime, testTime, saveTime)))
    //println(timeList.first())

    spark.createDataFrame(timeList, schema)
  }

  def loadToCache(df: DataFrame, spark: SparkSession): (Long, RDD[Vector]) = {
    time {
      val baseDS: RDD[Vector] = df.rdd.map(
        row => {
          val range = 0 until row.size
          val doublez: Array[Double] = range.map(i => {
              val x = row.getDouble(i)
              x
          }).toArray
          Vectors.dense(doublez)
        }
      )
      baseDS.cache()
    }
  }

  def train(df: RDD[Vector], spark: SparkSession): (Long, KMeansModel) = {
    time {
      KMeans.train(
        data = df,
        k = conf.k,
        maxIterations = conf.maxIterations,
        initializationMode = KMeans.K_MEANS_PARALLEL,
        seed = conf.seed )
    }
  }

  //Within Sum of Squared Errors
  def test(model: KMeansModel, df: RDD[Vector], spark: SparkSession): (Long, Double) = {
    time{ model.computeCost(df) }
  }

  def save(ds: RDD[Vector], model: KMeansModel, spark: SparkSession): (Long, Unit) = {
    val res = time {
      val vectorsAndClusterIdx: RDD[(String, Int)] = ds.map { point =>
        val prediction = model.predict(point)
        (point.toString, prediction)
      }
      import spark.implicits._
      // Already performed the match one level up so these are guaranteed to be Some(something)
      writeToDisk(conf.workloadResultsOutputDir.get, vectorsAndClusterIdx.toDF(), spark = spark)
    }
    ds.unpersist()
    res
  }

}
