package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_replace, lower, initcap, trim}

object SparkStreamingIngestionApp extends SparkRunning with App {
  running
}

trait SparkRunning {

  private lazy val DELTA_WORD_COUNT_TABLE_PATH : String = "tmp/delta/tables/word-count"
  private lazy val DELTA_WORD_COUNT_CHECKPOINT_PATH : String = "tmp/delta/_checkpoint/delta-word-event"
  

  lazy val running: Unit = {
    
    val spark = SparkSession.builder
      .appName("Spark Streaming Ingestion")
      .master("local[2]")
      .config("spark.jars.packages", "io.delta:delta-core_2.13:2.0.0")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    lazy val df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load



    val wordsDF = df
      .withColumn("value", regexp_replace($"value", "[.?,]", " "))
      .withColumn("value", regexp_replace($"value", "[-:]", " "))
      .withColumn("value", lower($"value"))
      .where("value != ''")
      .selectExpr("explode(split(value, ' ')) AS word")


    val wordCountsDF = wordsDF
      .withColumn("word", initcap(trim($"word")))
      .groupBy($"word")
      .count
      .orderBy($"count".desc)
      
    wordCountsDF.printSchema

    val queryStream = wordCountsDF.writeStream
      .format("delta")
      .outputMode("complete")
      .option("truncate", "false")
      .option("numRows", "25")
      .option("checkpointLocation", DELTA_WORD_COUNT_CHECKPOINT_PATH)
      .start(DELTA_WORD_COUNT_TABLE_PATH)

    println(">> Spark Streaming Ingestion")

    queryStream.awaitTermination
    spark.stop

  }
}
