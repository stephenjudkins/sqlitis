package sqlitis

import Sql._
import atto._
import Atto._
import atto.Parser.{Failure, Success, TResult}
import cats.data.NonEmptyList
import cats.implicits._

object Parser {

  private val letter = charRange('A' to 'Z') | charRange('a' to 'z')
  private val digit = charRange('0' to '9')

  private val literal = string("?").map(_ => Literal(()))


  private val idToken = (letter ~ many(letter | digit | char('_'))).map( { case (h, t) => s"$h${t.mkString}"})

  private val identifier:Parser[Identifier] =
    ((idToken <~ char('.')) ~ idToken).map { case (r, i) => Identifier(Some(r), i) } |
    idToken.map(Identifier(None, _)).filter(_.name.toLowerCase != "where")

  private val not:Parser[Expression[Unit]] = (kw("NOT") ~> expr).map(Not(_))

  private val functionCall:Parser[Expression[Unit]] = (identifier ~ parens(sepBy(expr, comma))).map { case (n, args) =>
    FunctionCall(n.name, args)
  }

  private val singleExpr:Parser[Expression[Unit]] = token(not | functionCall | identifier.widen[Expression[Unit]] | literal.widen[Expression[Unit]] | parens(expr))

  private val comma = token(string(","))

  private case class Op(
    sym: String,
    to: (Expression[Unit], Expression[Unit]) => Expression[Unit]
  )

  private case class Level(
    ops: NonEmptyList[Op],
    leftAssociative: Boolean
  )

  private def l(h: Op, t: Op*) = Level(NonEmptyList(h, t.toList), leftAssociative = true)
  private def r(h: Op, t: Op*) = Level(NonEmptyList(h, t.toList), leftAssociative = false)

  private val opsByPrecedence = List(
    l(Op("^", Exp[Unit])),
    l(Op("*", Mul[Unit]), Op("/", Div[Unit]), Op("%", Mod[Unit])),
    l(Op("+", Add[Unit]), Op("-", Sub[Unit])),
    r(Op("=", Equals[Unit]), Op("<>", NotEquals[Unit]), Op("!=", NotEquals[Unit])),
    l(Op("AND", And[Unit])),
    l(Op("OR", Or[Unit]))
  )

  private def kw(s: String) = token(stringCI(s))

  private def optKw[A](k: String, parser: Parser[A]):Parser[Option[A]] =
    opt(kw(k)).flatMap {
      case Some(_) => parser.map(Some(_))
      case None => ok(None)
    }

  implicit val expr:Parser[Expression[Unit]] = opsByPrecedence.foldLeft(singleExpr) { case (e, level) =>

    val op:Parser[Op] = choice(level.ops.map(o => stringCI(o.sym).map(_ => o))) <~ (whitespace ~ skipWhitespace)

    if (level.leftAssociative)
      (e ~ many(op ~ e)).map { case (i, t) =>
        t.foldLeft(i) { case (a, (o, b)) => o.to(a,b) }
      }
    else
      (many(e ~ op) ~ e).map { case (t, i) =>
        t.foldRight(i) { case ((a, o), b) => o.to(a,b) }
      }

  }.named("expr")

  private val field:Parser[Field[Unit]] = token(
    (expr ~ kw("AS") ~ identifier).map[Field[Unit]] { case ((e, _), i) => ExpressionField(e, Some(i.name)) } |
    (identifier <~ kw(".") <~ kw("*")).map[Field[Unit]](i => Splat(Some(i.name))) |
    kw("*").map[Field[Unit]](_ => Splat(None)) |
    expr.map[Field[Unit]](ExpressionField(_, None))
  )

  private val from = token[From](
    (identifier ~ whitespace ~ identifier).map { case ((t, _), a) => TableName(t.name, Some(a.name)) } |
    identifier.map(i => TableName(i.name, None))
  )

  private val orderBy = (
    expr ~ opt(kw("ASC").map(_ => true) | kw("DESC").map(_ => false)).map(_.getOrElse(false))
  ).map((OrderBy[Unit](_,_)).tupled)

  implicit val select:Parser[Select[Unit]] = (
    kw("SELECT") ~> opt(kw("DISTINCT")).map(_.isDefined) ~
      sepBy(field, comma) ~
      optKw("FROM", sepBy(from, comma)).map(_.getOrElse(Nil)) ~
      optKw("WHERE", expr) ~
      optKw("ORDER BY", orderBy) ~
      optKw("LIMIT", token(int))

  ).map { case (((((isDistinct, fields), from), where), orderBy), limit) =>
      Select(
        isDistinct = isDistinct,
        fields = fields,
        from = from,
        where = where,
        orderBy = orderBy,
        limit = limit
      )
  }

  private val s = whitespace ~> skipWhitespace

  implicit val insert:Parser[Insert[Unit]] = (
    kw("INSERT") ~> kw("INTO") ~> token(idToken) ~ parens(sepBy(token(idToken), comma)) ~ (s ~> kw("VALUES") ~> parens(sepBy(expr, comma)))
  ).map { case ((table, columns), values) => Sql.Insert(table, columns, values) }


}
