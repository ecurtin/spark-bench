package com.ibm.sparktc.sparkbench.sparklaunch

import java.io.File
import org.apache.spark.deploy.SparkSubmit
import scala.sys.process._
import scala.collection.parallel.ForkJoinTaskSupport

object SparkLaunch extends App {

  override def main(args: Array[String]): Unit = {
    assert(args.nonEmpty)
    val path = args.head
    val confSeq = SubmitConfigurator(new File(path))

    val sparkHome = sys.env("SPARK_HOME")

    val confSeqPar = confSeq.par
    //TODO address the concern that this could be confSeqPar.size threads for EACH member of ParSeq
    confSeqPar.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(confSeqPar.size))
    //TODO nope, this needs to launch new JVMs instead of just calling the method in a new thread
    confSeqPar.foreach( conf =>  s"""$sparkHome/bin/spark-submit ${conf.toSparkArgs()}""".!)


  }

}
