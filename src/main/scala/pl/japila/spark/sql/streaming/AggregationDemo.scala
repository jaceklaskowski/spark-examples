package pl.japila.spark.sql.streaming

/**
 * This is demo shows groupBy with no window(ing)
 *
 * @see https://books.japila.pl/spark-structured-streaming-internals/demo/streaming-aggregation/
 */
object AggregationDemo extends App {

  val appName = "Demo: Streaming Aggregation"
  println(s">>> [$appName] Starting up...")

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  singleStateStore(spark)

  // FIXME Consider JSON format for values
  // JSONified values would make more sense.
  // It'd certainly make the demo more verbose (extra JSON-specific "things")
  // but would likely ease building a connection between events on the command line and their DataFrame representation.

  import spark.implicits._
  import org.apache.spark.sql.functions._
  val events = spark
    .readStream
    .format("kafka")
    .option("subscribe", "demo.streaming-aggregation")
    .option("kafka.bootstrap.servers", ":9092")
    .load
    .select($"value" cast "string")
    .withColumn("tokens", split($"value", ","))
    .withColumn("id", $"tokens"(0))
    .withColumn("v", $"tokens"(1) cast "int")
    .withColumn("second", $"tokens"(2) cast "long")
    .withColumn("event_time", $"second" cast "timestamp") // Event time has to be a timestamp
    .select("id", "v", "second", "event_time")

  val grouped = events
    .withWatermark(eventTime = "event_time", delayThreshold = "10 seconds")
    .groupBy($"id")
    .agg(
      collect_list("v") as "vs",
      collect_list("second") as "seconds")

  import java.time.Clock

  val timeOffset = Clock.systemUTC.instant.getEpochSecond
  val queryName = s"Demo: Streaming Aggregation ($timeOffset)"
  val checkpointLocation = s"/tmp/demo-checkpoint-$timeOffset"

  import scala.concurrent.duration._
  import org.apache.spark.sql.streaming.OutputMode.Update
  import org.apache.spark.sql.streaming.Trigger

  val sq = grouped
    .writeStream
    .format("console")
    .option("checkpointLocation", checkpointLocation)
    .option("truncate", false)
    .outputMode(Update)
    .queryName(queryName)
    .trigger(Trigger.ProcessingTime(1.seconds))
    .start

  println(s">>> [$appName] Started")
  println(s">>> [$appName] You should see Batch: 0 in the console soon")

  sq.awaitTermination()
}
