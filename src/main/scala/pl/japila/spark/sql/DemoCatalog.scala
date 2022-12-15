package pl.japila.spark.sql

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

/**
 * @see https://books.japila.pl/spark-sql-internals/demo/developing-catalogplugin/
 */
class DemoCatalog extends CatalogPlugin
  with TableCatalog
  with SupportsNamespaces {

  val SUCCESS = true

  var _name: String = _
  var _options: CaseInsensitiveStringMap = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    import scala.jdk.CollectionConverters._
    println(s">>> initialize($name, ${options.asScala})")
    _name = name
    _options = options
  }

  override def name(): String = _name

  override def defaultNamespace(): Array[String] = {
    val default = super.defaultNamespace()
    val ns = if (default.isEmpty) {
      "<EMPTY>"
    } else {
      default.toSeq.mkString(".")
    }
    println(s"defaultNamespace=$ns")
    default
  }

  override def listNamespaces(): Array[Array[String]] = {
    println(">>> listNamespaces()")
    Array.empty
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    println(s">>> listNamespaces(namespace=${namespace.toSeq})")
    Array.empty
  }

  override def loadNamespaceMetadata(namespace: Array[String]): JMap[String, String] = {
    println(s">>> loadNamespaceMetadata(namespace=${namespace.toSeq})")
    import scala.jdk.CollectionConverters._
    Map.empty[String, String].asJava
  }

  override def createNamespace(namespace: Array[String], metadata: JMap[String, String]): Unit = {
    import scala.jdk.CollectionConverters._
    println(s">>> createNamespace(namespace=${namespace.toSeq}, metadata=${metadata.asScala})")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    println(s">>> createNamespace(namespace=${namespace.toSeq}, cascade=$cascade)")
    SUCCESS
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    println(s">>> alterNamespace(${namespace.toSeq}, $changes)")
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    println(s">>> listTables(${namespace.toSeq})")
    Array.empty
  }

  override def loadTable(ident: Identifier): Table = {
    println(s">>> loadTable($ident)")
    ???
  }

  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: JMap[String, String]): Table = {
    import scala.jdk.CollectionConverters._
    println(s">>> createTable($ident, $schema, $partitions, ${properties.asScala})")
    ???
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    println(s">>> alterTable($ident, $changes)")
    ???
  }

  override def dropTable(ident: Identifier): Boolean = {
    println(s">>> dropTable($ident)")
    SUCCESS
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    println(s">>> renameTable($oldIdent, $newIdent)")
  }

  override def toString = {
    import scala.jdk.CollectionConverters._

    val options = _options.entrySet().asScala
      .map(pair => pair.getKey + "=" + pair.getValue)
      .mkString(",")
    s"${this.getClass.getCanonicalName}(name=$name, options=$options)"
  }
}
