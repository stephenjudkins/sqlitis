package sqlitis

import sqlitis.Generator.Out
import sqlitis.Sql._

object Generator {

  sealed trait F[+A]
  case class S(v: String) extends F[Nothing]
  case class V[A](v: A)   extends F[A]

  type Out[A] = List[F[A]]

  private implicit class SqlHelper(val sc: StringContext) extends AnyVal {
    def sql[A](args: Out[A]*): Out[A] = {
      def impl(i: Int, parts: List[String], args: List[Out[A]], accum: List[F[A]]): Out[A] = {
        parts match {
          case s :: ss =>
            args match {
              case a :: as => impl(i + 1, ss, as, accum ::: S(s) :: a)
              case Nil     => impl(i + 1, ss, Nil, accum :+ S(s))
            }
          case Nil => accum
        }
      }

      impl(1, sc.parts.toList, args.toList, Nil)

    }
  }

  private def combine[A](a: List[Out[A]], sep: String): Out[A] = a match {
    case x :: xs => x ::: xs.flatMap(S(sep) :: _)
    case Nil     => Nil
  }

  def raw(s: String): Out[Nothing] = List(S(s))

  implicit object GenExpr extends Generator[Expression] {
    def binaryOp[A](op: BinaryOperator[A]): Out[A] = sql"(${gen(op.a)} ${raw(op.name)} ${gen(op.b)})"

    def gen[A](expr: Expression[A]): Out[A] =
      expr match {
        case Identifier(t, c) => {
          val prefix = t.map(s => s"$s.").getOrElse("")
          raw(s"$prefix$c")
        }
        case FunctionCall(f, args) => sql"${raw(f)}(${combine(args.map(gen), ", ")})"
        case Literal(a)            => List(V(a))
        case b: BinaryOperator[A]  => binaryOp(b)
        case IsNull(e)             => sql"(${gen(e)} IS NULL)"
        case Not(e)                => sql"(NOT ${gen(e)})"
      }
  }

  implicit object GenSelect extends Generator[Select] {

    def gen[A](s: Select[A]): Out[A] = {
      val clauses: List[Out[A]] = List(
        if (s.isDistinct) sql"DISTINCT" else sql"",
        combine(
          s.fields.map {
            case ExpressionField(e, o) => sql"${GenExpr.gen(e)}${raw(o.map(a => s" AS $a").getOrElse(""))}"
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
        s.where.map(w => sql"WHERE ${GenExpr.gen(w)}").getOrElse(raw("")),
        s.orderBy
          .map(o => sql"ORDER BY ${GenExpr.gen(o.e)} ${raw(if (o.asc) "ASC" else "DESC")}")
          .getOrElse(raw("")),
        s.limit.map(l => raw(s"LIMIT $l")).getOrElse(raw(""))
      )

      sql"SELECT ${combine(clauses.filter(_.nonEmpty), "\n")}"
    }
  }

  implicit object GenInsert extends Generator[Insert] {

    def gen[A](i: Insert[A]): Out[A] =
      sql"INSERT INTO ${raw(i.table)} (${raw(i.columns.mkString(", "))}) VALUES (${combine(i.values.map(GenExpr.gen), ", ")})"
  }

  case class Generated[A](
      sql: String,
      literals: List[A]
  )
}

trait Generator[F[_]] {
  def print[X](a: F[X]): String = generate(a)._2
  def gen[A](f: F[A]): Out[A]

  def generate[A](f: F[A], p: Int => String = _ => "?") = {
    import Generator._

    val out = gen(f)

    var i = 0

    val sql = out.map {
      case S(s) => s
      case V(_) => {
        i += 1
        p(i)
      }
    }.mkString

    val args = out.collect { case V(a) => a }

    (args, sql)
  }
}
