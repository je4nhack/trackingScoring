import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Date
import java.text.SimpleDateFormat

import com.je4npier.analytics.RunSparkDocker

object Test extends App {

  //RunSparkDocker.ejecutarConFe("trackingScoringV1.0.jar", "", "com.je4npier.trackingScoring.PruebaHadoop")
  RunSparkDocker.ejecutarConFe("trackingScoringV1.0.jar", "", "PruebaMongo")
  //pruebaEscritura
  /*
  @ActivateSegment
Feature: Feature for balanceParquetJob

Scenario: Test balanceParquetJob should result OK
  Given a config file path in HDFS hdfs://hadoop:9000/payrollAggregate.conf
  When execute payrollAggregate process in jar payrollAggregate-0.1.0-SNAPSHOT-jar-with-dependencies.jar with main class PayrollAggregateJob in Spark
  And read parquet file hdfs://localhost:9000/data/master/pdco/data/retailBusinessBanking/v_pdco_payroll_bdph_mov
  Then file should have 2312 registries
  Then file should have no registries with value 0 in column avg_local_payroll_amount
   */

}