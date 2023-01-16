package pl.japila.spark.sql.catalyst.expressions

object DemoDeclarativeAggregateTest extends App {

  import org.apache.spark.sql.SparkSession
  val appName = this.getClass.getSimpleName.replace("$", "")
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(appName)
    .getOrCreate()
  import spark.implicits._

  // Disable AQE since this is to test Streaming Aggregation in the end
  // SSS does not support AQE
  import org.apache.spark.sql.internal.SQLConf
  spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)

  // That's why Spark SQL comes with higher-level standard functions
  // so you don't even face such unpleasant moments
  // In Scala, there is 'implicit class' feature

  // With Struct data type for an aggregation buffer
  // and hence SortAggregate
  {
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

    // FIXME Why peak memory total (min, med, max )
    //       32.1 MiB (16.1 MiB, 16.1 MiB, 16.1 MiB )
    //       for SortExec
    //       Compare to the below run with HashAggregate

    q.explain(extended = true)
    //  println(q.rdd.toDebugString)

    spark.sparkContext.setJobDescription("(SortAggregate) Write")
    q.write.format("noop").mode("overwrite").save

    spark.sparkContext.setJobDescription("(SortAggregate) DataFrame.show")
    q.show()
  }

  // With simple (UnsafeRow-modifiable) data type for an aggregation buffer
  // and hence HashAggregate
  {
    val children = $"id".expr :: Nil
    val demoDA = DemoDeclarativeAggregate(children, numElements = 2, isStruct = false).toAggregateExpression

    import org.apache.spark.sql.Column

    val demo_agg = new Column(demoDA)

    val q = spark
      .range(start = 0, end = 5, step = 1, numPartitions = 2)
      .withColumn("gid", $"id" % 2)
      .groupBy($"gid")
      .agg(demo_agg)

    // FIXME Why peak memory total (min, med, max )
    //       512.0 KiB (256.0 KiB, 256.0 KiB, 256.0 KiB )
    //       Compare to the above run for SortAggregate

    q.explain(extended = true)
    //  println(q.rdd.toDebugString)

    spark.sparkContext.setJobDescription("(HashAggregate) Write")
    q.write.format("noop").mode("overwrite").save

    spark.sparkContext.setJobDescription("(HashAggregate) DataFrame.show")
    q.show()
  }

  println(">>> Pausing the current thread for 1 day")
  println(s">>> web UI available at ${spark.sparkContext.uiWebUrl.get}/SQL/")
  import java.util.concurrent.TimeUnit
  TimeUnit.DAYS.sleep(1)
}
