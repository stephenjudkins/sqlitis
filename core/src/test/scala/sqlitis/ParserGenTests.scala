package sqlitis

import Parser._
import sqlitis.Sql._
import utest._
import cats.parse.{Parser => CatsParser}

object ParserGenTests extends TestSuite {

  def testSql[A[_]](sql: String, ast: A[Unit])(implicit parser: CatsParser[A[Unit]], generator: Generator[A]) = {
    val parsed = parser.parseAll(sql)

    val (_, generated) = generator.generate(ast)

    parsed match {
      case Right(actual) => assert(actual == ast)
      case Left(error) => {
        sys.error(s"Failed after ${sql.take(error.failedAtOffset)}; offsets = ${error.offsets.toList.mkString(",")}")
      }
    }
    assert(parsed == Right(ast))
    val parsedAgain = parser.parseAll(generated)
    assert(parsedAgain == Right(ast))
  }

  // TODO: shapeless bullshit to make A[C] into A[Unit]
  def testSqlWithCaptures[A[_], C](sql: String, ast: A[C], captures: List[C])(implicit
      parser: CatsParser[A[Unit]],
      generator: Generator[A]
  ) = {
    val parsed                      = parser.parseAll(sql)
    val (actualCaptures, generated) = generator.generate(ast)
    assert(captures == actualCaptures)
    val parsedAgain = parser.parseAll(generated)
    assert(parsed == parsedAgain)

  }

  def tests: Tests =
    Tests {
      "identifier" - {
        testSql[Expression]("a", Identifier("a"))
        testSql[Expression]("x.y", Identifier(Some("x"), "y"))
        testSql[Expression]("abcd", Identifier("abcd"))
        testSql[Expression]("A42", Identifier("A42"))
        testSql[Expression]("A_B", Identifier("A_B"))
      }
      "AND" - {
        testSql[Expression]("a AND b", And(Identifier("a"), Identifier("b")))
      }
//      "caseInsensitive" - {
//        testSql[Expression]("a and b", And(Identifier("a"), Identifier("b")))
//      }
      "leftAssociativity" - {
        testSql[Expression]("a AND b AND c", And(And(Identifier("a"), Identifier("b")), Identifier("c")))
      }
      "rightAssociativity" - {
        testSql[Expression]("a = b = c", Equals(Identifier("a"), Equals(Identifier("b"), Identifier("c"))))
      }
      "precedence" - {
        testSql[Expression]("a OR b AND c", Or(Identifier("a"), And(Identifier("b"), Identifier("c"))))
      }
      "()" - {
        testSql[Expression]("(a OR b) AND c", And(Or(Identifier("a"), Identifier("b")), Identifier("c")))
      }
      "function call" - {
        testSql[Expression](
          "booFooWoo(x,y,z)",
          FunctionCall("booFooWoo", List(Identifier("x"), Identifier("y"), Identifier("z")))
        )
      }
      "* +" - {
        testSql[Expression]("a* b + c", Add(Mul(Identifier("a"), Identifier("b")), Identifier("c")))
      }
      "NOT" - {
        testSql[Expression]("NOT x", Not(Identifier("x")))
      }
      "SELECT" - {
        testSql[Select](
          "SELECT spam.foo AS zomg, eggs.bar AS wtf FROM t1 spam, t2 eggs WHERE green = red ORDER BY bar ASC LIMIT 420",
          Select(
            fields = List(
              ExpressionField(Identifier(Some("spam"), "foo"), Some("zomg")),
              ExpressionField(Identifier(Some("eggs"), "bar"), Some("wtf"))
            ),
            from = List(TableName("t1", Some("spam")), TableName("t2", Some("eggs"))),
            where = Some(Equals(Identifier("green"), Identifier("red"))),
            orderBy = Some(OrderBy(Identifier("bar"), asc = true)),
            limit = Some(420)
          )
        )
      }
      "INSERT" - {
        testSql[Insert](
          "INSERT INTO foo (a,b,c) VALUES (x,y,z)",
          Sql.Insert(
            table = "foo",
            columns = List("a", "b", "c"),
            values = List(Identifier("x"), Identifier("y"), Identifier("z"))
          )
        )
      }
      "captures" - {
        testSqlWithCaptures(
          "SELECT x FROM t WHERE t.a = ? AND t.b = ? AND t.c = ?",
          Select[Int](
            fields = List(ExpressionField(Identifier("x"), None)),
            from = List(TableName("t", None)),
            where = Some(
              And(
                And(
                  Equals(Identifier(Some("t"), "a"), Literal(1)),
                  Equals(Identifier(Some("t"), "b"), Literal(2))
                ),
                Equals(Identifier(Some("t"), "c"), Literal(3))
              )
            )
          ),
          List(1, 2, 3)
        )
      }

    }
}
