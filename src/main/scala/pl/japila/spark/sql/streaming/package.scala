package pl.japila.spark.sql

import org.apache.spark.sql.SparkSession

package object streaming {

  def singleStateStore(spark: SparkSession): Unit = {
    val numShufflePartitions = 1
    import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
    println(s">>> Changing $SHUFFLE_PARTITIONS to $numShufflePartitions for a single StateStore")
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, numShufflePartitions)
    assert(spark.sessionState.conf.numShufflePartitions == numShufflePartitions)
  }
}
