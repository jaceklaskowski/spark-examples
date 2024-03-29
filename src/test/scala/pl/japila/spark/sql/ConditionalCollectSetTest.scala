package pl.japila.spark.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.CaseWhen

import scala.reflect.internal.util.TableDef.Column

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
  val emptyState = Seq
    .empty[(String, String, Boolean, Boolean, Boolean)]
    .toDF("code", "date", "pdd", "currently_pdd", "dor")

  val stateBatch1 = {
    val parcelStatuses = Seq(
      Parcel(code = "001", statuses = Seq(
        Status(status = "PDD", date = "2022-12-06 12:00"))
      )
    ).toDF
    microBatch(parcelStatuses, emptyState)
  }
  println("After Batch 1:")
  stateBatch1.show(truncate = false)

  val stateBatch2 = {
    val parcelStatuses = Seq(
      Parcel(code = "001", statuses = Seq(
        Status(status = "PDD", date = "2022-12-06 12:00"),
        Status(status = "DOR", date = "2022-12-06 13:00")
      ))
    ).toDF
    microBatch(parcelStatuses, stateBatch1)
  }
  println("After Batch 2:")
  stateBatch2.show(truncate = false)

  val stateBatch3 = {
    val parcelStatuses = Seq(
      Parcel(code = "002", statuses = Seq(
        Status(status = "PDD", date = "2022-12-06 12:00")
      ))
    ).toDF
    microBatch(parcelStatuses, stateBatch2)
  }
  println("After Batch 3:")
  stateBatch3.show(truncate = false)

  val lastBatch = stateBatch3

  println("Solution:")
  import org.apache.spark.sql.functions._
  val solution = lastBatch
    .groupBy("code")
    .agg(
      last("pdd") as "pdd", // FIXME assumes proper order so most likely incorrect
      last("currently_pdd") as "currently_pdd", // FIXME assumes proper order so most likely incorrect
      // if ever delivered, it's delivered and cannot be "resurrected" as "pdd" or "currently_pdd"
      exists(collect_set("dor"), identity) as "dor",
    )
  solution.show(truncate = false)

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
  def microBatch(parcelStatuses: DataFrame, state: DataFrame): DataFrame = {

    println("state:")
    state.show(truncate = false)

    import org.apache.spark.sql.functions._
    val exploded = parcelStatuses
      .withColumn("exploded", explode($"statuses"))
      .select("code", "exploded.*")

    println("parcelStatuses exploded:")
    exploded.show(truncate = false)

    val joined = exploded.join(state, Seq("code"), "left")
    println("joined:")
    joined.show(truncate = false)

    val filled = joined
      .na.fill(false, Seq("pdd", "currently_pdd", "dor"))
      .select(
        $"code",
        exploded("date"),
        expr(
          """
            |CASE
            |   WHEN (status = 'PDD' AND !pdd) THEN true -- turn it on
            |   ELSE pdd                                 -- keep the current pdd state
            |END
            |""".stripMargin) as "pdd",
        expr(
          """
            |CASE
            |   WHEN (status = 'PDD' AND !currently_pdd) THEN true -- PDD turns CURR_PDD on
            |   WHEN (status = 'DOR' AND currently_pdd) THEN false -- DOR turns CURR_PDD off
            |   ELSE currently_pdd                                 -- keep the current pdd state
            |END
            |""".stripMargin) as "currently_pdd",
        ($"status" === "DOR") as "dor"
      )
    println("filled:")
    filled.show(truncate = false)

    val unmodified_codes = state
      .join(parcelStatuses, Seq("code"), "left_anti")
      .select(state.columns.map(col): _*)

    filled
      .orderBy($"code", $"date".asc)
      .groupBy("code")
      .agg(
        last("date") as "date",
        last("pdd") as "pdd",
        last("currently_pdd") as "currently_pdd",
        last("dor") as "dor",
      )
      .unionByName(unmodified_codes)
  }
}
