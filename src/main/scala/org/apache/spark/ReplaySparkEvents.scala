package org.apache.spark

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.util.JsonProtocol

import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream

/**
 * @see https://learn.microsoft.com/en-us/azure/databricks/kb/clusters/replay-cluster-spark-events
 */
object ReplaySparkEvents extends App {

  import org.apache.spark.sql.SparkSession

  implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val eventlogs = args(0)
  replaySparkEvents(eventlogs)

  println("Processing eventlogs DONE")
  println(s"Open up the web UI at ${spark.sparkContext.uiWebUrl.get} and start exploring...")

  println("Pausing the current thread for 1 day")
  TimeUnit.DAYS.sleep(1)

  def replaySparkEvents(pathToEventLogs: String)(implicit spark: SparkSession): Unit = {
    val eventLogFiles = new File(pathToEventLogs)
      .listFiles()
      .toSeq
      .filter(_.getName.startsWith("eventlog"))
      .map(_.getPath)
      .sorted
    require(eventLogFiles.nonEmpty, "No event logs found at this path")

    // All eventlog files end with YYYY-MM-DD--HH-MM.gz except the last one
    // The last eventlog file is eventlog
    // eventlog is the very recent eventlog file and should be the last
    val inOrder = eventLogFiles.tail ++ Seq(eventLogFiles.head)

    val lineIterator = inOrder.iterator.map { file =>
      val path = new Path(file)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      val fileStream = fs.open(path)
      val stream = if (file.endsWith(".gz")) {
        new GZIPInputStream(fileStream)
      } else {
        fileStream
      }
      println(s"Processing $file")
      val lines = IOUtils.readLines(stream, Charset.forName("utf-8"))
      (stream, lines)
    }

    import scala.jdk.CollectionConverters._
    val lines = lineIterator.iterator.flatMap { p => p._2.asScala }
    val streams = lineIterator.map(_._1)

    val unrecognizedEvents = new scala.collection.mutable.HashSet[String]
    val unrecognizedProperties = new scala.collection.mutable.HashSet[String]

    val sc = spark.sparkContext
    val listenerBus = sc.listenerBus

    import com.fasterxml.jackson.core.JsonParseException
    import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
    import org.json4s.jackson.JsonMethods._

    var line: String = null
    try {
      while (lines.hasNext) {
        try {
          val entry = lines.next()
          line = entry
          listenerBus.post(JsonProtocol.sparkEventFromJson(parse(line)))
        } catch {
          case e: java.lang.ClassNotFoundException =>
            // Ignore unknown events, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unknown event.
            if (!unrecognizedEvents.contains(e.getMessage)) {
              println(s"Drop unrecognized event: ${e.getMessage}")
              unrecognizedEvents.add(e.getMessage)
            }
            println(s"Drop incompatible event log: $line")
          case e: UnrecognizedPropertyException =>
            // Ignore unrecognized properties, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unrecognized property.
            if (!unrecognizedProperties.contains(e.getMessage)) {
              println(s"Drop unrecognized property: ${e.getMessage}")
              unrecognizedProperties.add(e.getMessage)
            }
            println(s"Drop incompatible event log: $line")
          case jpe: JsonParseException =>
            // We can only ignore exception from last line of the file that might be truncated
            // the last entry may not be the very last line in the event log, but we treat it
            // as such in a best effort to replay the given input
            if (lines.hasNext) {
              throw jpe
            } else {
              println(s"Got JsonParseException from log file. The file might not have finished writing cleanly.")
            }
        }
      }
    } finally {
      streams.foreach(IOUtils.closeQuietly)
    }
  }

}
