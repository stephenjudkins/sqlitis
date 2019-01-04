package sqlitis

import Sql._
import atto._, Atto._
import cats.implicits._

object Parser {

  private val letter = charRange('A' to 'Z') | charRange('a' to 'z')
  private val digit = charRange('0' to '9')

  private def literal = string("?").map(_ => Literal)


  private def idToken = (letter ~ many(letter | digit | char('_'))).map( { case (h, t) => s"$h${t.mkString}"})

  private def identifier:Parser[Identifier] =
    (idToken ~ char('.').void ~ idToken).map { case ((r, _), i) => Identifier(Some(r), i) } |
    idToken.map(Identifier(None, _))

  private def singleExpr:Parser[Expression] = token(not | functionCall | identifier.widen[Expression] | literal.widen[Expression] | parens(expr))

  private def not:Parser[Expression] = (kw("NOT") ~> expr).map(Not(_))

  private def functionCall:Parser[Expression] = (identifier ~ parens(sepBy(expr, token(char(','))))).map { case (n, args) =>
    FunctionCall(n.name, args)
  }

  private def comma = token(string(","))

  private def byPrecedence:List[(String, (Expression, Expression) => Expression)] = List(
    "*" -> (Mul(_, _)),
    "+" -> (Add(_, _)),
    "<>" -> (NotEquals(_, _)),
    "!=" -> (NotEquals(_, _)),
    "=" -> (Equals(_, _)),
    "AND" -> (And(_,_)),
    "OR" -> (Or(_,_))
  )


  private def orderBy = (
    expr ~ opt(kw("ASC").map(_ => true) | kw("DESC").map(_ => false)).map(_.getOrElse(false))
  ).map((OrderBy(_,_)).tupled)

  private def kw(s: String) = token(stringCI(s))

  private def optKw[A](k: String, parser: Parser[A]):Parser[Option[A]] =
    opt(kw(k)).flatMap {
      case Some(_) => parser.map(Some(_))
      case None => ok(None)
    }

  implicit val expr:Parser[Expression] = byPrecedence.foldLeft(singleExpr) { case (e, (name, toExpr)) =>
    sepBy1(e, stringCI(name) <~ many1(whitespace)).map(_.reduceLeft(toExpr)).named(name)
  }

  private def field:Parser[Field] = token(
    (expr ~ kw("AS") ~ identifier).map[Field] { case ((e, _), i) => ExpressionField(e, Some(i.name)) } |
    (identifier <~ kw(".") <~ kw("*")).map[Field](i => Splat(Some(i.name))) |
    kw("*").map[Field](_ => Splat(None)) |
    expr.map[Field](ExpressionField(_, None))
  )

  private def from = token[From](
    (identifier ~ whitespace ~ identifier).map { case ((t, _), a) => TableName(t.name, Some(a.name)) } |
    identifier.map(i => TableName(i.name, None))
  )

  implicit val select:Parser[Select] = (
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


}
