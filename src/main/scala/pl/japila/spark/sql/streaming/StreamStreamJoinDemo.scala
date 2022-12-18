package pl.japila.spark.sql.streaming

object StreamStreamJoinDemo extends App {

  val appName = "Demo: Stream-Stream Join"
  print(s">>> [$appName] Starting up...")

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  val numShufflePartitions = 1
  import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
  spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, numShufflePartitions)
  assert(spark.sessionState.conf.numShufflePartitions == numShufflePartitions)

  val options = Map(
    "kafka.bootstrap.servers" -> ":9092"
  )

  val customers = spark
    .readStream
    .format("kafka")
    .options(options)
    .option("subscribe", "demo.stream-stream-join.customers")
    .load
    .select(
      $"key" cast "string" cast "long" as "id",
      $"value" cast "string" as "name")

  // root
  // |-- id: long (nullable = true)
  // |-- name: string (nullable = true)

  import org.apache.spark.sql.types._
  val valueSchema = StructType(Seq(
    StructField("customer_id", LongType),
    StructField("total", DoubleType))
  )
  import org.apache.spark.sql.functions.from_json
  val id = $"key" cast "string" as "id"
  val customer_total = from_json($"value" cast "string", valueSchema)

  val transactions = spark
    .readStream
    .format("kafka")
    .options(options)
    .option("subscribe", "demo.stream-stream-join.transactions")
    .load
    .withColumn("customer_total", customer_total)
    .select(id, $"customer_total.*")

  // root
  // |-- id: string (nullable = true)
  // |-- customer_id: long (nullable = true)
  // |-- total: double (nullable = true)

  val joined = transactions
    .join(customers)
    .where(transactions("customer_id") === customers("id"))
    .select(
      customers("name") as "customer_name",
      transactions("total"))

  import java.time.Clock
  val timeOffset = Clock.systemUTC.instant.getEpochSecond
  val queryName = s"$appName ($timeOffset)"
  val checkpointLocation = s"target/demo-stream-stream-join-checkpoint-$timeOffset"

  import org.apache.spark.sql.streaming.Trigger
  import scala.concurrent.duration._
  val sq = joined
    .writeStream
    .format("console")
    .option("checkpointLocation", checkpointLocation)
    .queryName(queryName)
    .trigger(Trigger.ProcessingTime(1.second))
    .start

  println(s"DONE")
  println(s">>> [$appName] You should soon see Batch: 0 in the console")

  sq.awaitTermination()
}
