package sqlitis

object Sql {

  sealed trait Statement

  case class Select(
    isDistinct: Boolean = false,
    fields: List[Field] = Nil,
    from: List[From] = Nil,
    where: Option[Expression] = None,
    orderBy: Option[OrderBy] = None,
    limit: Option[Int] = None
  ) extends Statement

  sealed trait Field
  case class ExpressionField(e: Expression, as: Option[String]) extends Field
  case class Splat(table: Option[String]) extends Field

  sealed trait ColumnIdentifier
  case class Named(name: String) extends ColumnIdentifier
  case object All extends ColumnIdentifier

  sealed trait From
  case class TableName(name: String, alias: Option[String]) extends From


  sealed trait Expression
  case class Identifier(relation: Option[String], name: String) extends Expression
  object Identifier {
    def apply(name: String):Identifier = Identifier(None, name)
  }
  case class FunctionCall(function: String, args: List[Expression]) extends Expression
  case object Literal extends Expression

  abstract class BinaryOperator(val name: String) extends Expression {
    def a: Expression
    def b: Expression
  }

  case class And(a: Expression, b: Expression) extends BinaryOperator("AND")
  case class Or(a: Expression, b: Expression) extends BinaryOperator("OR")
  case class IsNull(e: Expression) extends Expression
  case class Not(e: Expression) extends Expression
  case class Equals(a: Expression, b: Expression) extends BinaryOperator("=")
  case class NotEquals(a: Expression, b: Expression) extends BinaryOperator("!=")

  case class Mul(a: Expression, b: Expression) extends BinaryOperator("*")
  case class Div(a: Expression, b: Expression) extends BinaryOperator("/")
  case class Mod(a: Expression, b: Expression) extends BinaryOperator("%")

  case class Add(a: Expression, b: Expression) extends BinaryOperator("+")
  case class Sub(a: Expression, b: Expression) extends BinaryOperator("-")

  case class Exp(a: Expression, b: Expression) extends BinaryOperator("^")

  case class OrderBy(e: Expression, asc: Boolean)


  case class Insert(
    table: String,
    columns: List[String],
    values: List[Expression]
  )
}
