package pl.japila.spark.sql.streaming

object StreamStreamJoinDemo extends App {

  val appName = "Demo: Stream-Stream Join"
  println(s">>> [$appName] Starting up...")

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  val options = Map(
    "kafka.bootstrap.servers" -> ":9092"
  )
  val left = spark
    .readStream
    .format("kafka")
    .options(options)
    .option("subscribe", "demo.stream-stream-join.left")
    .load
    .select(
      $"key" cast "string",
      $"value" cast "string")
  val right = spark
    .readStream
    .format("kafka")
    .options(options)
    .option("subscribe", "demo.stream-stream-join.right")
    .load
    .select(
      $"key" cast "string",
      $"value" cast "string")

  val joined = left.join(right)
    .where(left("key") === right("key"))

  import java.time.Clock
  val timeOffset = Clock.systemUTC.instant.getEpochSecond
  val queryName = s"$appName ($timeOffset)"
  val checkpointLocation = s"target/demo-stream-stream-join-checkpoint-$timeOffset"

  import org.apache.spark.sql.streaming.Trigger
  import scala.concurrent.duration._
  val sq = joined.writeStream
    .format("console")
    .option("checkpointLocation", checkpointLocation)
    .queryName(queryName)
    .trigger(Trigger.ProcessingTime(1.second))
    .start

  println(s">>> [$appName] Started")

  sq.awaitTermination()
}
