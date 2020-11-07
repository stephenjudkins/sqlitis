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

  private def parens[A](p: P[A]): Parser1[A] = char('(') *> p <* char(')')

  private def token[A](p: Parser1[A]) = p <* maybeWhitespace

  private val letter                    = charWhere(_.isLetter)
  private val whitespace: Parser1[Unit] = charsWhile1(_.isWhitespace).void
  private val maybeWhitespace: P[Unit]  = charsWhile(_.isWhitespace).void
  private val comma                     = string1(",") <* maybeWhitespace
  private val literal                   = string1("?").as(Literal(()))

  private val keywords                = Set("WHERE") // TODO: all of these
  private val keywordP: Parser1[Unit] = oneOf1(keywords.map(string1).toList)

  private val idToken: Parser1[String] =
    (!keywordP).with1 *> (
      letter ~ charsWhile(c => c.isLetterOrDigit || c == '_')
    ).string

  private val identifier: Parser1[Identifier] =
    (idToken ~ (char('.') *> idToken).?).map {
      case (r, Some(i)) => Identifier(Some(r), i)
      case (i, None)    => Identifier(None, i)
    }

  private val not: Parser1[Expression[Unit]] = (kw("NOT") *> expr).map(Not(_))

  private val functionCall: Parser1[Expression[Unit]] =
    (identifier ~ parens(repSep(expr, 0, comma))).map { case (n, args) =>
      FunctionCall(n.name, args)
    }

  private val singleExpr: Parser1[Expression[Unit]] =
    token(
      P.oneOf1(
        not ::
          functionCall.backtrack ::
          identifier ::
          literal ::
          parens(expr) ::
          Nil
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

  private def kw(s: String): Parser1[String] = token(stringCI(s)).backtrack

  private def optKw[A](k: String, parser: P[A]): P[Option[A]] =
    (kw(k) *> parser).? <* maybeWhitespace

  private val field: Parser1[Field[Unit]] =
    token(
      P.oneOf1[Field[Unit]](
        (expr ~ (kw("AS") *> idToken).?).map { case (e, as) => ExpressionField(e, as) } ::
          (idToken <* kw(".*")).map(t => Splat(Some(t))) ::
          kw("*").as(Splat(None)) ::
          Nil
      )
    )

  private val from =
    token[From](
      (identifier ~ whitespace ~ identifier).map { case ((t, _), a) => TableName(t.name, Some(a.name)) } |
        identifier.map(i => TableName(i.name, None))
    )

  private val orderBy = (
    expr ~ (kw("ASC").as(true) | kw("DESC").as(false)).backtrack.?.map(_.getOrElse(false))
  ).map { case (e, asc) => OrderBy[Unit](e, asc) }

  private val int: Parser1[Int] = Numbers.signedIntString.map(_.toInt)

  private val opsByPrecedence =
    List(
      l(Op("^", Exp[Unit])),
      l(Op("*", Mul[Unit]), Op("/", Div[Unit]), Op("%", Mod[Unit])),
      l(Op("+", Add[Unit]), Op("-", Sub[Unit])),
      r(Op("=", Equals[Unit]), Op("<>", NotEquals[Unit]), Op("!=", NotEquals[Unit])),
      l(Op("AND", And[Unit])),
      l(Op("OR", Or[Unit]))
    )

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

  implicit val select: Parser1[Select[Unit]] = kw("SELECT") *> (
    (kw("DISTINCT")).?.map(_.isDefined),
    repSep(field, 0, comma),
    optKw("FROM", repSep(from, 1, comma)).map(_.getOrElse(Nil)),
    optKw("WHERE", expr <* maybeWhitespace),
    optKw("ORDER BY", orderBy),
    optKw("LIMIT", token(int))
  ).mapN { case (isDistinct, fields, from, where, orderBy, limit) =>
    Select(
      isDistinct = isDistinct,
      fields = fields,
      from = from,
      where = where,
      orderBy = orderBy,
      limit = limit
    )
  }

  implicit val insert: Parser1[Insert[Unit]] = kw("INSERT") *> (
    kw("INTO") *> token(idToken),
    parens(rep1Sep(token(idToken), 1, comma)),
    (whitespace *> kw("VALUES") *> parens(repSep(expr, 1, comma)))
  ).mapN { case (table, columns, values) => Sql.Insert(table, columns.toList, values) }

}
