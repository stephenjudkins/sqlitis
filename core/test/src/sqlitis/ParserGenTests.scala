package sqlitis

import Parser._
import atto.{Parser => AttoParser, Atto}
import Atto._
import sqlitis.Sql._
import utest._

object ParserGenTests extends TestSuite {

  def testSql[A: Generator : AttoParser](sql: String, ast: A) = {
    val parsed = implicitly[AttoParser[A]].parseOnly(sql).either
    val generated = implicitly[Generator[A]].print(ast)
    println(generated)

    assert(parsed == Right(ast))
    val parsedAgain = implicitly[AttoParser[A]].parseOnly(generated).either
    assert(parsedAgain == Right(ast))
  }

  def tests: Tests = Tests {
    'identifier - {
      testSql[Expression]("a", Identifier("a"))
      testSql[Expression]("x.y", Identifier(Some("x"), "y"))
      testSql[Expression]("abcd", Identifier("abcd"))
      testSql[Expression]("A42", Identifier("A42"))
    }
    'AND - {
      testSql[Expression]("a AND b ", And(Identifier("a"), Identifier("b")))
    }
    'leftAssociativity - {
      testSql[Expression]("a AND b AND c", And(And(Identifier("a"), Identifier("b")), Identifier("c")))
    }
    'rightAssociativity - {
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
    'NOT - {
      testSql[Expression]("NOT x", Not(Identifier("x")))
    }
    'SELECT - {
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


  }
}
