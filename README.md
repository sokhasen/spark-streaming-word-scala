# spark-streaming-word-scala

#### Run
- publish socket: `nc -lk 9999`
- Build spark app : `sbt package`
- Run spark app : `sbt run` or `spark-submit --master local[1] --packages io.delta:delta-core_2.13:2.0.0 --deploy-mode client  target/scala-2.13/spark-delta-streaming-[ingestion|console]_2.13-0.1.0-SNAPSHOT.jar`
