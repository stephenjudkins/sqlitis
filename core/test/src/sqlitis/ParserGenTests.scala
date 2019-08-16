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
      testSql[Expression[Unit]]("a", Identifier("a"))
      testSql[Expression[Unit]]("x.y", Identifier(Some("x"), "y"))
      testSql[Expression[Unit]]("abcd", Identifier("abcd"))
      testSql[Expression[Unit]]("A42", Identifier("A42"))
    }
    'AND - {
      testSql[Expression[Unit]]("a AND b ", And(Identifier("a"), Identifier("b")))
    }
    'leftAssociativity - {
      testSql[Expression[Unit]]("a AND b AND c", And(And(Identifier("a"), Identifier("b")), Identifier("c")))
    }
    'rightAssociativity - {
      testSql[Expression[Unit]]("a = b = c", Equals(Identifier("a"), Equals(Identifier("b"), Identifier("c"))))
    }
    "precedence" - {
      testSql[Expression[Unit]]("a OR b AND c", Or(Identifier("a"), And(Identifier("b"), Identifier("c"))))
    }
    "()" - {
      testSql[Expression[Unit]]("(a OR b) AND c", And(Or(Identifier("a"), Identifier("b")), Identifier("c")))
    }
    "function call" - {
      testSql[Expression[Unit]]("booFooWoo(x,y,z)", FunctionCall("booFooWoo", List(Identifier("x"), Identifier("y"), Identifier("z"))))
    }
    "* +" - {
      testSql[Expression[Unit]]("a* b + c", Add(Mul(Identifier("a"), Identifier("b")), Identifier("c")))
    }
    'NOT - {
      testSql[Expression[Unit]]("NOT x", Not(Identifier("x")))
    }
    'SELECT - {
      testSql[Select[Unit]](
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
    'INSERT - {
      testSql[Insert[Unit]](
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
