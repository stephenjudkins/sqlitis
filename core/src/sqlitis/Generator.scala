package sqlitis

import sqlitis.Sql._

object Generator {


  implicit object GenExpr extends Generator[Expression] {
    def binaryOp(op: BinaryOperator): String = s"(${print(op.a)} ${op.name} ${print(op.b)})"

    def print(expr: Expression): String = expr match {
      case Identifier(t, c) => {
        val prefix = t.map(s => s"$s.").getOrElse("")
        s"$prefix$c"
      }
      case FunctionCall(f, args) => s"$f(${args.map(print).mkString(", ")})"
      case Literal => "?"
      case b: BinaryOperator => binaryOp(b)
      case IsNull(e) => s"(${print(e)} IS NULL)"
      case Not(e) => s"(NOT ${print(e)})"
    }
  }

  implicit object GenSelect extends Generator[Select] {
    def print(s: Select): String = {
      val clauses = Seq(
        if (s.isDistinct) "DISTINCT" else "",
        s.fields.map {
          case ExpressionField(e, o) => s"${GenExpr.print(e)}${o.map(a => s" AS $a").getOrElse("")}"
          case Splat(Some(t)) => s"$t.*"
          case Splat(None) => "*"
        }.mkString(", "),
        if (s.from.nonEmpty) s"FROM\n${s.from.map {
          case TableName(t, Some(a)) => s"$t $a"
          case TableName(t, None) => t
        }.mkString(", ")}" else "",
        s.where.map(w => s"WHERE ${GenExpr.print(w)}").getOrElse(""),
        s.orderBy.map(o => s"ORDER BY ${GenExpr.print(o.e)} ${if (o.asc) "ASC" else "DESC"}").getOrElse(""),
        s.limit.map(l => s"LIMIT $l").getOrElse("")
      )

      s"SELECT ${clauses.mkString("\n")}".trim
    }
  }


}

trait Generator[A] {
  def print(a: A):String
}
