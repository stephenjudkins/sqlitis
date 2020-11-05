package sqlitis

import Parser._
import atto.{Atto, Parser => AttoParser}
import Atto._
import sqlitis.Generator.SqlHelper
import sqlitis.Sql._
import utest._

object ParserGenTests extends TestSuite {

  def testSql[A[_]](sql: String, ast: A[Unit])(implicit parser: AttoParser[A[Unit]], generator: Generator[A]) = {
    val parsed = parser.parseOnly(sql).either
    val (_, generated) = generator.generate(ast)

    assert(parsed == Right(ast))
    val parsedAgain = parser.parseOnly(generated).either
    assert(parsedAgain == Right(ast))
  }

  // TODO: shapeless bullshit to make A[C] into A[Unit]
  def testSqlWithCaptures[A[_], C](sql: String, ast: A[C], captures: List[C])(implicit parser: AttoParser[A[Unit]], generator: Generator[A]) = {
    val parsed = parser.parseOnly(sql).either
    val (actualCaptures, generated) = generator.generate(ast)

    assert(captures == actualCaptures.reverse)

    val parsedAgain = parser.parseOnly(generated).either

    assert(parsed == parsedAgain)

  }

  def tests: Tests = Tests {
    "captures" - {
      testSqlWithCaptures(
        "SELECT x FROM t WHERE t.a = ? AND t.b = ? AND t.c = ?",
        Select[Int](
          fields = List(ExpressionField(Identifier("x"), None)),
          from = List(TableName("t", None)),
          where = Some(And(
            And(
              Equals(Identifier(Some("t"), "a"), Literal(1)),
              Equals(Identifier(Some("t"), "b"), Literal(2)),
            ),
            Equals(Identifier(Some("t"), "c"), Literal(3)),
          )
        )),
        List(1, 2, 3)
      )
    }
    "identifier" - {
      testSql[Expression]("a", Identifier("a"))
      testSql[Expression]("x.y", Identifier(Some("x"), "y"))
      testSql[Expression]("abcd", Identifier("abcd"))
      testSql[Expression]("A42", Identifier("A42"))
    }
    "AND" - {
      testSql[Expression]("a AND b ", And(Identifier("a"), Identifier("b")))
    }
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
      testSql[Expression]("booFooWoo(x,y,z)", FunctionCall("booFooWoo", List(Identifier("x"), Identifier("y"), Identifier("z"))))
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
          fields = List(ExpressionField(Identifier(Some("spam"), "foo"), Some("zomg")), ExpressionField(Identifier(Some("eggs"), "bar"), Some("wtf"))),
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


  }
}
