package pl.japila.spark.sql.catalyst.expressions

object DemoDeclarativeAggregateTest extends App {

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  // That's why Spark SQL comes with higher-level standard functions
  // so you don't even face such unpleasant moments
  // In Scala, there is 'implicit class' feature

  val children = $"id".expr :: Nil
  val demoDA = DemoDeclarativeAggregate(children, numElements = 2).toAggregateExpression
  import org.apache.spark.sql.Column
  val demo_agg = new Column(demoDA)

  val q = spark
    // Using 2 partitions to include Exchange in the query plan
    // Otherwise, Spark would place two SortAggregate's one after another
    .range(start = 0, end = 5, step = 1, numPartitions = 2)
    .withColumn("gid", $"id" % 2)
    .groupBy($"gid")
    .agg(demo_agg)

  q.explain(extended = true)
//  println(q.rdd.toDebugString)

  q.write.format("noop").mode("overwrite").save

  println(">>> Pausing the current thread for 1 day")
  println(s">>> Open up the web UI at ${spark.sparkContext.uiWebUrl.get} and start exploring...")
  import java.util.concurrent.TimeUnit
  TimeUnit.DAYS.sleep(1)
}
