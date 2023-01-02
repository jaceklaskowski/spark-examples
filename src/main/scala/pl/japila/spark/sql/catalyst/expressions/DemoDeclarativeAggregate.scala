package pl.japila.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CreateStruct, Expression, Literal}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

case class DemoDeclarativeAggregate(children: Seq[Expression], numElements: Int = 1)
  extends DeclarativeAggregate {

  // All vals are initialized when DemoDeclarativeAggregate is created
  // Just reminding myself...

  override val prettyName: String = "demo_declarative_aggregate"

  private val _nullable = false

  val dataType = StructType(
    StructField("id", LongType, nullable = false) ::
    Nil
  )
  protected lazy val registry = AttributeReference("registry", dataType, nullable = _nullable)()

  override val initialValues: Seq[Expression] = {
    println(">>> initialValues")
    import org.apache.spark.sql.functions._
    /* registry = */ CreateStruct.create(Literal(0L) :: Nil) :: Nil
  }

  override val updateExpressions: Seq[Expression] = {
    println(">>> updateExpressions")
    /* registry = */ CreateStruct.create(Literal(1L) :: Nil) :: Nil
  }

  override val mergeExpressions: Seq[Expression] = {
    println(">>> mergeExpressions")
    /* registry = */ CreateStruct.create(Literal(2L) :: Nil) :: Nil
  }

  override val evaluateExpression: Expression = {
    println(">>> evaluateExpression")
    /* registry = */ CreateStruct.create(Literal(3L) :: Nil)
  }
  override def aggBufferAttributes: Seq[AttributeReference] = {
    println(s">>> aggBufferAttributes: $registry")
//    new Exception("aggBufferAttributes").printStackTrace()
    registry :: Nil
  }

  override def nullable: Boolean = {
    println(s">>> nullable: ${_nullable}")
    _nullable
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    println(">>> withNewChildrenInternal")
    copy(children = newChildren)
  }
}
