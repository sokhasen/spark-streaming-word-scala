package example

import org.apache.spark.sql.SparkSession

object SparkStreamingConsoleApp extends SparkRunning with App {
  
  running
}

trait SparkRunning {
  private lazy val DELTA_WORD_COUNT_TABLE_PATH : String = "../spark-delta-streaming-ingestion/tmp/delta/tables/word-count"
  
  lazy val running: Unit = {

    lazy val spark = SparkSession.builder
      .appName("Spark Streaming Delta Console")
      .master("local[1]")
      .config("spark.jars.packages", "io.delta:delta-core_2.13:2.0.0")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val wordCountsDF = spark.readStream
      .format("delta")
      .option("ignoreChanges","true")
      .load(DELTA_WORD_COUNT_TABLE_PATH)

      wordCountsDF.printSchema

      val queryStream = wordCountsDF
        .writeStream
        .format("console")
        .outputMode("update")
        .option("numRows", "100")
        .option("truncate", "false")
        .start

    println("Spark Streaming Console")
    queryStream.awaitTermination

    spark.stop
  }
}
