package com.je4npier.analytics

import com.mongodb.spark._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

import scala.language.postfixOps
import scala.sys.process._


object RunSparkDocker {

  def ejecutarConFe (jarName: String, pathConfig : String, mainClass : String) : Int = {

    val SPARK_HOME = "/usr/spark-2.1.0"

    // Activate only for remote debug.
    val REMOTE_DEBUG_ENABLED = false
    val REMOTE_DEBUG_LINE = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 "

    val extractDockerID = ("docker ps -a" #| "grep je4n-tesis_master" #| Seq("awk" , "{print $1}") !!).trim

    val sparkSubmit = s"$SPARK_HOME/bin/spark-submit "
    var conf = " --conf spark.sql.shuffle.partitions=3 --conf spark.eventLog.enabled=true "
    if (REMOTE_DEBUG_ENABLED) {
      conf = s"$conf --driver-java-options $REMOTE_DEBUG_LINE"
    }

    val jarPath = s"/classpath/${jarName}"
    val sparkOpts = s"--class $mainClass $conf $jarPath"

    val runSparkInDocker = if(REMOTE_DEBUG_ENABLED) {
      s"SPARK_PRINT_LAUNCH_COMMAND=true $sparkSubmit $sparkOpts $pathConfig"
    } else {
      s"$sparkSubmit $sparkOpts $pathConfig"
    }

    println("docker", "exec", "-i",extractDockerID, "sh", "-c", runSparkInDocker)
    val dockerExec = Seq("docker", "exec", "-i",extractDockerID, "sh", "-c", runSparkInDocker)

    val result = dockerExec.!

    result

    0

  }
}