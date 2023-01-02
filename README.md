# Apache Spark Examples

This project contains Apache Spark applications to showcase notable features of [Spark SQL](https://books.japila.pl/spark-sql-internals/) and [Spark Structured Streaming](https://books.japila.pl/spark-structured-streaming-internals/). _Enjoy!_

## DemoDeclarativeAggregate

[DemoDeclarativeAggregateTest](src/test/scala/pl/japila/spark/sql/catalyst/expressions/DemoDeclarativeAggregateTest.scala)

## StreamStreamJoinDemo

Source Code: [StreamStreamJoinDemo.scala](src/main/scala/pl/japila/spark/sql/streaming/StreamStreamJoinDemo.scala)

Register a customer (with ID 1).

```shell
echo "1:Customer One" | kcat -P -b :9092 -K: -t demo.stream-stream-join.customers
```

Register a transaction of the user.

```shell
kcat -P -b :9092 \
  -t demo.stream-stream-join.transactions \
  -k 1 \
  src/test/resources/transactions/1.json
```

Register another transaction of the user.

```shell
kcat -P -b :9092 \
  -t demo.stream-stream-join.transactions \
  -k 1 \
  src/test/resources/transactions/2.json
```
