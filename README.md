# Apache Spark Examples

This project contains Apache Spark applications to showcase notable features of [Spark SQL](https://books.japila.pl/spark-sql-internals/) and [Spark Structured Streaming](https://books.japila.pl/spark-structured-streaming-internals/). _Enjoy!_

## StreamStreamJoinDemo

Source Code: [StreamStreamJoinDemo.scala](src/main/scala/pl/japila/spark/sql/streaming/StreamStreamJoinDemo.scala)

```shell
echo "1:1" | kcat -P -b :9092 -K : -t demo.stream-stream-join.left
echo "1:1" | kcat -P -b :9092 -K : -t demo.stream-stream-join.right
```