import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Date
import java.text.SimpleDateFormat

import com.mongodb.spark.MongoSpark

object PruebaMongo extends App {
  import org.apache.spark.sql.SparkSession

  val sparkSession = SparkSession.builder()
    .appName("example-spark-scala-read-and-write-from-mongo")
    // Configuration for writing in a Mongo collection
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/USRTANALYT")
    .config("spark.mongodb.output.collection", "scoringPeriod")
    // Configuration for reading a Mongo collection
    .config("spark.mongodb.input.uri", "mongodb://mongo:27017/USRTANALYT")
    .config("spark.mongodb.input.collection", "scoring")
    // Type of Partitionner to use to transform Documents to dataframe
    .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
    // Number of partitions in the resulting dataframe
    .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "1")
    .getOrCreate()

  val documents = sparkSession.sparkContext.parallelize(
    Seq(
      Row("first", 2.0, 7.0),
      Row("second", 3.5, 2.5),
      Row("third", 7.0, 5.9)
    )
  )

  val schema = new StructType()
    .add(StructField("id", StringType, true))

  val df = sparkSession.createDataFrame(documents, schema)

  df.show

  MongoSpark.save(df.write.mode("overwrite"))

  val df2 = MongoSpark.load(sparkSession)
  df2.show

}