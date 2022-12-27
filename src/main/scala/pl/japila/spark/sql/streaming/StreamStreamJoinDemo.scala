package pl.japila.spark.sql.streaming

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * @see https://books.japila.pl/spark-structured-streaming-internals/demo/stream-stream-inner-join
 */
object StreamStreamJoinDemo extends App {

  val appName = "Demo: Stream-Stream Inner Join"
  print(s">>> [$appName] Starting up...")

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  import org.apache.spark.sql.execution.streaming.MemoryStream
  implicit val sqlContext = spark.sqlContext

  // https://inpost.pl/aktualnosci-czym-jest-mpok
  case class MPok(id: Long, mpokOriginTimestamp: Timestamp, aggregateId: Long)
  val mpoks = MemoryStream[MPok](numPartitions = 1)
  val mpokStream = mpoks.toDF()
    .withWatermark("mpokOriginTimestamp", "3 minutes")
    .as("mpok")

  case class Parcel(id: Long, wasInMpok: Boolean, mpokId: Long, parcelOriginTimestamp: Timestamp)
  val parcels = MemoryStream[Parcel](numPartitions = 1)
  val parcelStream = parcels.toDF()
    .filter($"wasInMpok")
    .filter($"mpokId".isNotNull)
    .withWatermark("parcelOriginTimestamp", "1 day")
    .as("parcelEvent")

  import org.apache.spark.sql.functions.expr
  val joinExprs = expr(
    """
      mpokId = mpok.aggregateId AND
      parcelOriginTimestamp >= mpokOriginTimestamp AND
      parcelOriginTimestamp <= mpokOriginTimestamp + interval 1 days
    """
  )
  val joined = mpokStream.join(parcelStream, joinExprs, joinType = "inner")

  import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
  val tableName = "output_table"
  val sq = joined
    .writeStream
    .format("memory")
    .queryName(tableName)
    .start
    .asInstanceOf[StreamingQueryWrapper]
    .streamingQuery

  println(s"DONE")

  print(s">>> [$appName] Sending out events...")
  mpoks.addData {
    MPok(0, Timestamp.valueOf(LocalDateTime.of(2022, 12, 1, 0, 0, 1)), aggregateId = 0)
  }
  parcels.addData {
    Parcel(0, wasInMpok = true, mpokId = 0, parcelOriginTimestamp = Timestamp.valueOf(LocalDateTime.of(2022, 12, 1, 0, 0, 1)))
  }
  println(s"DONE")

  sq.processAllAvailable()

  import org.apache.spark.sql.execution.streaming.sources.MemorySink
  val memorySink = sq.sink.asInstanceOf[MemorySink]
  println("toDebugString:\n" + memorySink.toDebugString)

  // FIXME Start testing the output using output_table table
  spark.table(tableName).show(truncate = false)
}
