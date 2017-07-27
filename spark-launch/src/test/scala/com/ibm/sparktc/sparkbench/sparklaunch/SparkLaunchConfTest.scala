package com.ibm.sparktc.sparkbench.sparklaunch

import java.io.File

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SparkLaunchConfTest extends FlatSpec with Matchers with BeforeAndAfter {

  // dirty hack to test environment variable overrides
  private def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  private def removeEnv(key: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.remove(key)
  }

  before {
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()
    removeEnv("SPARK_CONF_ENV_OVERRIDES")
    removeEnv("SPARK_MASTER_HOST")
  }

  "SparkLaunchConf" should "turn into arguments properly" in {

    val relativePath = "/etc/sparkConfTest.conf"
    val resource = new File(getClass.getResource(relativePath).toURI)
//    val source = scala.io.Source.fromFile(resource)
    val (sparkContextConfs, _) = SparkLaunch.mkConfs(resource)
    val conf1 = sparkContextConfs.head._1

    val expectedSparkConfs = Array(
      "--conf", "spark.shuffle.service.enabled=false",
      "--conf", "spark.fake=yes",
      "--conf", "spark.dynamicAllocation.enabled=false"
    )

    conf1.sparkConfs shouldBe expectedSparkConfs
    conf1.sparkArgs should contain ("--master")

    SparkLaunch.rmTmpFiles(sparkContextConfs.map(_._2))

//    val resultConf = conf1.createSparkContext().sparkContext.getConf
//    resultConf.getBoolean("spark.dynamicAllocation.enabled", defaultValue = true) shouldBe false
//    resultConf.getBoolean("spark.shuffle.service.enabled", defaultValue = true) shouldBe false
//    resultConf.get("spark.fake") shouldBe "yes"
  }

  it should "not blow up when spark context confs are left out" in {
    val relativePath = "/etc/testConfFile1.conf"
    val resource = new File(getClass.getResource(relativePath).toURI)
    setEnv("SPARK_MASTER_HOST", "yarn")

    val (sparkContextConfs, _) = SparkLaunch.mkConfs(resource)
    val conf2 = sparkContextConfs.head._1

    conf2.sparkConfs.isEmpty shouldBe true
    conf2.sparkArgs should contain ("--master")

    SparkLaunch.rmTmpFiles(sparkContextConfs.map(_._2))
  }

  it should "get environment variable conf overrides in a simple case" in {
    setEnv("SPARK_CONF_ENV_OVERRIDES", "key1=value1;key2=value2")
    val confs = SparkLaunchConf.getEnvConfs
    confs.get("key1") shouldBe Some("value1")
    confs.get("key2") shouldBe Some("value2")
  }

  it should "get environment variable conf overrides in a failure case" in {
    setEnv("SPARK_CONF_ENV_OVERRIDES", "key1=value1;key2=value2=value2")
    val confs = SparkLaunchConf.getEnvConfs
    confs.get("key1") shouldBe Some("value1")
    confs should not contain ("key2")
  }

  private def testConf(confs: Array[String], k: String, toMatch: String): Unit = {
    confs.find(_.contains(k)) match {
      case Some(str) => str shouldBe toMatch
      case _ => fail(s"$k not found in conf")
    }
  }

  it should "build the conf without env variable overrides" in {
    val relativePath = "/etc/testConfFileForOverrides.conf"
    val resource = new File(getClass.getResource(relativePath).toURI)
    setEnv("SPARK_MASTER_HOST", "yarn")

    val (sparkContextConfs, _) = SparkLaunch.mkConfs(resource)
    val conf2 = sparkContextConfs.head._1

    conf2.sparkConfs should have size (8)
    testConf(conf2.sparkConfs, "key0", "key0=value0")
    testConf(conf2.sparkConfs, "key1", "key1=valuex")
    testConf(conf2.sparkConfs, "key2", "key2=valuey")
    testConf(conf2.sparkConfs, "key3", "key3=value3")

    conf2.sparkArgs should contain ("--master")
    conf2.sparkArgs should contain ("yarn")

    SparkLaunch.rmTmpFiles(sparkContextConfs.map(_._2))
  }

  it should "build the conf with env variable overrides" in {
    val relativePath = "/etc/testConfFileForOverrides.conf"
    val resource = new File(getClass.getResource(relativePath).toURI)
    setEnv("SPARK_MASTER_HOST", "yarn")
    setEnv("SPARK_CONF_ENV_OVERRIDES", "key1=value1;key2=value2")

    val (sparkContextConfs, _) = SparkLaunch.mkConfs(resource)
    val conf2 = sparkContextConfs.head._1

    conf2.sparkConfs should have size 8
    testConf(conf2.sparkConfs, "key0", "key0=value0")
    testConf(conf2.sparkConfs, "key1", "key1=value1")
    testConf(conf2.sparkConfs, "key2", "key2=value2")
    testConf(conf2.sparkConfs, "key3", "key3=value3")

    conf2.sparkArgs should contain ("--master")
    conf2.sparkArgs should contain ("yarn")

    SparkLaunch.rmTmpFiles(sparkContextConfs.map(_._2))
  }
}