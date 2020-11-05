package sqlitis

import sqlitis.Generator.Out
import sqlitis.Sql._

object Generator {

  type Out[A] = (List[A], String)

  private implicit class SqlHelper(val sc: StringContext) extends AnyVal {
    def sql[A](args: Out[A]*): Out[A] = {
      def impl(parts: List[String], args: List[Out[A]], accumA: List[A], accumS: List[String]): Out[A] =
        parts match {
          case s :: ss =>
            args match {
              case a :: as => impl(ss, as, a._1 ::: accumA, a._2 :: s :: accumS)
              case Nil     => impl(ss, Nil, accumA, s :: accumS)
            }
          case Nil => (accumA, accumS.reverse.mkString)
        }

      impl(sc.parts.toList, args.toList, Nil, Nil)
    }
  }

  def combine[A](a: List[Out[A]], sep: String = "") = (a.flatMap(_._1), a.map(_._2).mkString(sep))

  def raw(s: String) = (Nil, s)

  implicit object GenExpr extends Generator[Expression] {
    def binaryOp[A](op: BinaryOperator[A]): Out[A] = sql"(${generate(op.a)} ${raw(op.name)} ${generate(op.b)})"

    def generate[A](expr: Expression[A]): Out[A] =
      expr match {
        case Identifier(t, c) => {
          val prefix = t.map(s => s"$s.").getOrElse("")
          raw(s"$prefix$c")
        }
        case FunctionCall(f, args) => sql"${raw(f)}(${combine(args.map(generate), ", ")})"
        case Literal(a)            => (List(a), "?")
        case b: BinaryOperator[A]  => binaryOp(b)
        case IsNull(e)             => sql"(${generate(e)} IS NULL)"
        case Not(e)                => sql"(NOT ${generate(e)})"
      }
  }

  implicit object GenSelect extends Generator[Select] {

    def generate[A](s: Select[A]): Out[A] = {
      val clauses: List[Out[A]] = List(
        if (s.isDistinct) sql"DISTINCT" else sql"",
        combine(
          s.fields.map {
            case ExpressionField(e, o) => sql"${GenExpr.generate(e)}${raw(o.map(a => s" AS $a").getOrElse(""))}"
            case Splat(Some(t))        => raw(s"$t.*")
            case Splat(None)           => raw("*")
          },
          ", "
        ),
        if (s.from.nonEmpty) sql"FROM ${combine(
          s.from.map {
            case TableName(t, Some(a)) => raw(s"$t $a")
            case TableName(t, None)    => raw(t)
          },
          ", "
        )}"
        else raw(""),
        s.where.map(w => sql"WHERE ${GenExpr.generate(w)}").getOrElse(raw("")),
        s.orderBy
          .map(o => sql"ORDER BY ${GenExpr.generate(o.e)} ${raw(if (o.asc) "ASC" else "DESC")}")
          .getOrElse(raw("")),
        s.limit.map(l => raw(s"LIMIT $l")).getOrElse(raw(""))
      )

      sql"SELECT ${combine(clauses.filter(_._2.nonEmpty), "\n")}"
    }
  }

  implicit object GenInsert extends Generator[Insert] {

    def generate[A](i: Insert[A]): (List[A], String) =
      sql"INSERT INTO ${raw(i.table)} (${raw(i.columns.mkString(", "))}) VALUES (${combine(i.values.map(GenExpr.generate), ", ")})"
  }

  case class Generated[A](
      sql: String,
      literals: List[A]
  )
}

trait Generator[F[_]] {
  def print[X](a: F[X]): String = generate(a)._2
  def generate[A](f: F[A]): Out[A]
}
