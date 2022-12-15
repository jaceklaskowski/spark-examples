name := "spark-examples"
organization := "pl.japila.spark"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.5"

val sparkVersion = "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
