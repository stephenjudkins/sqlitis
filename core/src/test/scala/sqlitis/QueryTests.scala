package sqlitis

import sqlitis.Query.{Ref => _, _}
import sqlitis.Sql._
import sqlitis.util.TableToQuery
import utest._

object QueryTests extends TestSuite {

  type Ref[A] = sqlitis.Query.Ref[Unit, A]
  def Ref[A](e: Expression[Unit]) = sqlitis.Query.Ref[Unit, A](e)

  case class TableA[C <: Ctx](
      a: C#NoDefault[Int],
      b: C#NoDefault[String]
  )
  implicit object TableA extends Table[TableA] {
    val schema = TableA[Ctx.Schema](
      a = Column[Int]("a"),
      b = Column[String]("b")
    )

    val name = "foo"
  }

  case class TableB[C <: Ctx](
      x: C#NoDefault[Int],
      y: C#NoDefault[String],
      z: C#NoDefault[Int]
  )

  implicit object TableB extends Table[TableB] {
    val schema = TableB[Ctx.Schema](
      x = Column[Int]("x"),
      y = Column[String]("y"),
      z = Column[Int]("z")
    )

    val name = "bar"
  }

  import TestBackend._

  def tests: Tests =
    Tests {
      "Querify" - {
        val q = TableToQuery[Unit, TableA]("foo_42", TableA.schema)(TableToQuery.instance)

        assert(
          q == TableA[Ctx.Queried[Unit]](
            a = Ref[Int](Identifier(Some("foo_42"), "a")),
            b = Ref[String](Identifier(Some("foo_42"), "b"))
          )
        )
      }

      "queryRef" - {
        val q = Query[TableA].map(_.a)

        val o   = select(q)
        val sql = o.sql

        implicitly[o.type <:< TestResult[Int]]

        val expected = Select(
          fields = List(ExpressionField(Identifier(Some("foo"), "a"), None)),
          from = List(TableName("foo", None))
        )

        assert(sql == expected)

      }

      "queryHList" - {
        import shapeless._
        val q = Query[TableA].map(t => t.a :: t.b :: HNil)

        val o = select(q)

        implicitly[o.type <:< TestResult[Int :: String :: HNil]]

        val expected = Select(
          fields = List(
            ExpressionField(Identifier(Some("foo"), "a"), None),
            ExpressionField(Identifier(Some("foo"), "b"), None)
          ),
          from = List(TableName("foo", None))
        )

        assert(o.sql == expected)

      }

      "queryTuple" - {

        val q = for {
          a1 <- Query[TableA]
          a2 <- Query[TableA] if a1.a === a2.a
        } yield (a1.a, a2.b)

        implicitly[q.type <:< Query[Unit, (Ref[Int], Ref[String])]]

        val o = select(q)

        implicitly[o.type <:< TestResult[(Int, String)]]

        val sql = o.sql
        val expected = Select(
          fields = List(
            ExpressionField(Identifier(Some("foo"), "a"), None),
            ExpressionField(Identifier(Some("foo_1"), "b"), None)
          ),
          from = List(TableName("foo", None), TableName("foo", Some("foo_1"))),
          where = Some(Equals(Identifier(Some("foo"), "a"), Identifier(Some("foo_1"), "a")))
        )
        assert(sql == expected)

//      println(Generator.GenSelect.print(o.sql))
      }

      "queryNestedTuple" - {
        val q = Query[TableB].map(t => ((t.x, t.y), t.z))

        implicitly[q.type <:< Query[Unit, ((Ref[Int], Ref[String]), Ref[Int])]]

        val o = select(q)

        implicitly[o.type <:< TestResult[((Int, String), Int)]]

        val sql = o.sql

        val expected = Select(
          fields = List(
            ExpressionField(Identifier(Some("bar"), "x"), None),
            ExpressionField(Identifier(Some("bar"), "y"), None),
            ExpressionField(Identifier(Some("bar"), "z"), None)
          ),
          from = List(TableName("bar", None))
        )
        assert(sql == expected)

      }

      "queryTable" - {
        val o = select(Query[TableA])

        val sql = o.sql

        implicitly[o.type <:< TestResult[TableA[Ctx.Concrete]]]

        val expected = Select(
          fields = List(
            ExpressionField(Identifier(Some("foo"), "a"), None),
            ExpressionField(Identifier(Some("foo"), "b"), None)
          ),
          from = List(TableName("foo", None))
        )
        assert(sql == expected)

//      println(Generator.GenSelect.print(o.sql))

      }

      "filter" - {

        val q = for {
          ta <- Query[TableA]
          tb <- Query[TableB] if ta.a === tb.x
        } yield (ta.b, tb.y)

        val sql = select(q).sql

        val expected = Select(
          fields = List(
            ExpressionField(Identifier(Some("foo"), "b"), None),
            ExpressionField(Identifier(Some("bar"), "y"), None)
          ),
          from = List(TableName("foo", None), TableName("bar", None)),
          where = Some(Equals(Identifier(Some("foo"), "a"), Identifier(Some("bar"), "x")))
        )

        assert(sql == expected)

//      println(Generator.GenSelect.print(sql))

      }

    }

}
