package sqlitis

object Sql {

  sealed trait Statement[+A]

  case class Select[+A](
      isDistinct: Boolean = false,
      fields: List[Field[A]] = Nil,
      from: List[From] = Nil,
      where: Option[Expression[A]] = None,
      orderBy: Option[OrderBy[A]] = None,
      limit: Option[Int] = None
  ) extends Statement[A] {
    def reverseFields = copy(fields = fields.reverse)
  }

  sealed trait Field[+A]
  case class ExpressionField[+A](e: Expression[A], as: Option[String]) extends Field[A]
  case class Splat(table: Option[String])                              extends Field[Nothing]

  sealed trait ColumnIdentifier
  case class Named(name: String) extends ColumnIdentifier
  case object All                extends ColumnIdentifier

  sealed trait From
  case class TableName(name: String, alias: Option[String]) extends From

  sealed trait Expression[+A]
  case class Identifier(relation: Option[String], name: String) extends Expression[Nothing]
  object Identifier {
    def apply(name: String): Identifier = Identifier(None, name)
  }
  case class FunctionCall[+A](function: String, args: List[Expression[A]]) extends Expression[A]
  case class Literal[A](a: A)                                              extends Expression[A]

  abstract class BinaryOperator[+A](val name: String) extends Expression[A] {
    def a: Expression[A]
    def b: Expression[A]
  }

  case class And[A](a: Expression[A], b: Expression[A])       extends BinaryOperator[A]("AND")
  case class Or[A](a: Expression[A], b: Expression[A])        extends BinaryOperator[A]("OR")
  case class IsNull[A](e: Expression[A])                      extends Expression[A]
  case class Not[A](e: Expression[A])                         extends Expression[A]
  case class Equals[A](a: Expression[A], b: Expression[A])    extends BinaryOperator[A]("=")
  case class NotEquals[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("!=")

  case class Mul[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("*")
  case class Div[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("/")
  case class Mod[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("%")

  case class Add[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("+")
  case class Sub[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("-")

  case class Exp[A](a: Expression[A], b: Expression[A]) extends BinaryOperator[A]("^")

  case class OrderBy[+A](e: Expression[A], asc: Boolean)

  case class Insert[+A](
      table: String,
      columns: List[String],
      values: List[Expression[A]]
  ) extends Statement[A]
}
