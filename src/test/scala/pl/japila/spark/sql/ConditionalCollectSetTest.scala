package pl.japila.spark.sql

import org.apache.spark.sql.DataFrame

object ConditionalCollectSetTest extends App {

  import org.apache.spark.sql.SparkSession

  val appName = this.getClass.getSimpleName.replace("$", "")
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(appName)
    .getOrCreate()
  import spark.implicits._

  case class Status(status: String, date: String)
  case class Parcel(code: String, statuses: Seq[Status])
  val parcelStatuses = Seq(
    Parcel(code = "001", statuses = Seq(
      Status(status = "PDD", date = "2022-12-06 12:00"))
    )
  ).toDF
  val emptyState = Seq
    .empty[(String, String, Boolean, Boolean, Boolean)]
    .toDF("code", "date", "pdd", "currently_pdd", "dor")

  batch1(parcelStatuses, emptyState).show(truncate = false)

  println(">>> Pausing the current thread for 1 day")
  println(s">>> web UI available at ${spark.sparkContext.uiWebUrl.get}/SQL/")
  import java.util.concurrent.TimeUnit
  println("FIXME sleep")
//  TimeUnit.DAYS.sleep(1)

  /**
   * Groups parcels by status date to the following columns:
   *    - pdd
   *    - currently_pdd
   *    - dor
   */
  def batch1(batchDF: DataFrame, state: DataFrame): DataFrame = {
    println("parcelStatuses:")
    batchDF.show(truncate = false)

    println("state:")
    state.show(truncate = false)

    import org.apache.spark.sql.functions._
    val exploded = batchDF
      .withColumn("exploded", explode($"statuses"))
      .select("code", "exploded.*")

    println("exploded:")
    exploded.show(truncate = false)

    val joined = exploded.join(state, Seq("code"), "left")
      .na.fill(false, Seq("pdd", "currently_pdd", "dor"))
      .select(
        $"code",
        exploded("date"),
        when($"status" === "PDD" && !$"pdd", true) as "pdd",
        when($"status" === "PDD" && !$"currently_pdd", true) as "currently_pdd",
        $"dor"
      )
    joined.cache().count()
    println("joined (cached):")
    joined.show(truncate = false)

    joined
      .groupBy("date")
      .agg(
        collect_set(struct("code", "pdd")) as "pdds",
        collect_set(struct("code", "currently_pdd")) as "currently_pdds",
        collect_set(struct("code", "dor")) as "dors",
      )
  }
}
