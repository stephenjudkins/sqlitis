package sqlitis

import Sql._
import cats.parse.{Numbers, Parser1, Parser => P}
import cats.data.NonEmptyList
import cats.implicits._

import scala.collection.immutable.NumericRange

object Parser {
  import P._

  private implicit class ParserOps[A](val p: Parser1[A]) extends AnyVal {
    def |[B >: A](q: Parser1[B]): Parser1[B] = p.backtrack.orElse1(q)
  }

  private def stringCI(s: String): Parser1[String] = {
    string1(s).as(s)
  }

  private def parens[A](p: P[A]): Parser1[A] = string1("(") *> p <* string1(")")

  private def token[A](p: Parser1[A]) = p <* maybeWhitespace

  private def letter = charWhere(_.isLetter)

  private def comma   = string1(",") <* maybeWhitespace
  private def literal = string1("?").as(Literal(()))

  private val keywords = Set("WHERE") // TODO: all of these

  lazy val idToken =
    (
      letter ~ charsWhile(c => c.isLetterOrDigit || c == '_')
    ).string.flatMap(s => if (keywords(s.toUpperCase)) P.fail else P.pure(s))

  private lazy val identifier: Parser1[Identifier] =
    ((idToken <* char('.')) ~ idToken).map { case (r, i) => Identifier(Some(r), i) } |
      idToken.map(Identifier(None, _))

  private lazy val not: Parser1[Expression[Unit]] = (kw("NOT") *> expr).map(Not(_))

  private lazy val functionCall: Parser1[Expression[Unit]] =
    (identifier ~ parens(repSep(expr, 0, comma))).map { case (n, args) =>
      FunctionCall(n.name, args)
    }

  lazy val singleExpr: Parser1[Expression[Unit]] =
    token(
      P.oneOf1(
        List(
          not,
          functionCall,
          identifier,
          literal,
          parens(expr)
        ).map(_.backtrack)
      )
    )

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

  private lazy val opsByPrecedence =
    List(
      l(Op("^", Exp[Unit])),
      l(Op("*", Mul[Unit]), Op("/", Div[Unit]), Op("%", Mod[Unit])),
      l(Op("+", Add[Unit]), Op("-", Sub[Unit])),
      r(Op("=", Equals[Unit]), Op("<>", NotEquals[Unit]), Op("!=", NotEquals[Unit])),
      l(Op("AND", And[Unit])),
      l(Op("OR", Or[Unit]))
    )

  private def kw(s: String): Parser1[String] = token(stringCI(s)).backtrack

  private def optKw[A](k: String, parser: P[A]): P[Option[A]] =
    kw(k).?.flatMap {
      case Some(_) => parser.map(Some(_))
      case None    => P.pure(None)
    } <* maybeWhitespace

  private lazy val whitespace: Parser1[Unit] = charsWhile1(_.isWhitespace).void

  private lazy val maybeWhitespace: P[Unit] = charsWhile(_.isWhitespace).void

  private lazy val field: Parser1[Field[Unit]] =
    token(
      (expr ~ kw("AS") ~ identifier).map[Field[Unit]] { case ((e, _), i) => ExpressionField(e, Some(i.name)) } |
        (identifier <* kw(".") <* kw("*")).map[Field[Unit]](i => Splat(Some(i.name))) |
        kw("*").map[Field[Unit]](_ => Splat(None)) |
        expr.map[Field[Unit]](ExpressionField(_, None))
    )

  private lazy val from =
    token[From](
      (identifier ~ whitespace ~ identifier).map { case ((t, _), a) => TableName(t.name, Some(a.name)) } |
        identifier.map(i => TableName(i.name, None))
    )

  private lazy val orderBy = (
    expr ~ (kw("ASC").as(true) | kw("DESC").as(false)).backtrack.?.map(_.getOrElse(false))
  ).map { case (e, asc) => OrderBy[Unit](e, asc) }

  private lazy val int: Parser1[Int] = Numbers.signedIntString.map(_.toInt)

  implicit lazy val expr: Parser1[Expression[Unit]] =
    defer1(
      opsByPrecedence
        .foldLeft[Parser1[Expression[Unit]]](singleExpr) { case (e, level) =>
          val op: Parser1[Op] = (
            P.oneOf1(level.ops.toList.map(o => (string1(o.sym)).as(o))) <* whitespace
          ).backtrack

          if (level.leftAssociative)
            (e ~ (op ~ e).rep).map { case (i, t) =>
              t.foldLeft(i) { case (a, (o, b)) => o.to(a, b) }
            }
          else
            ((e ~ op).backtrack.rep.with1 ~ e).map { case (t, i) =>
              t.foldRight(i) { case ((a, o), b) => o.to(a, b) }
            }

        }
    )

  implicit lazy val select: P[Select[Unit]] = (
    kw("SELECT") *> (kw("DISTINCT")).?.map(_.isDefined) ~
      repSep(field, 0, comma) ~
      optKw("FROM", repSep(from, 1, comma)).map(_.getOrElse(Nil)) ~
      optKw("WHERE", expr <* maybeWhitespace) ~
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

  implicit lazy val insert: P[Insert[Unit]] = (
    kw("INSERT") *> kw("INTO") *> token(idToken) ~ parens(rep1Sep(token(idToken), 1, comma)) ~ (whitespace *> kw(
      "VALUES"
    ) *> parens(
      repSep(expr, 1, comma)
    ))
  ).map { case ((table, columns), values) => Sql.Insert(table, columns.toList, values) }

}
