package pl.japila.spark.sql

import org.apache.spark.sql.SparkSession

package object streaming {

  def oneShufflePartition(spark: SparkSession): Unit = {
    val numShufflePartitions = 1
    import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, numShufflePartitions)
    assert(spark.sessionState.conf.numShufflePartitions == numShufflePartitions)
  }
}
