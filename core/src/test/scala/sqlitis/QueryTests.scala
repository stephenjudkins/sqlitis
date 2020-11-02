package sqlitis

import shapeless.Id
import sqlitis.Query._
import sqlitis.Sql._
import utest._

object QueryTests extends TestSuite {


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
    y: C#NoDefault[String]
  )

  implicit object TableB extends Table[TableB] {
    val schema = TableB[Ctx.Schema](
      x = Column[Int]("x"),
      y = Column[String]("y")
    )

    val name = "bar"
  }



  def tests: Tests = Tests {
    'Querify - {
      val q = Querify("foo_42", TableA.schema)

      assert(q == TableA[Ctx.Queried](
        a = Ref[Int](Identifier(Some("foo_42"), "a")),
        b = Ref[String](Identifier(Some("foo_42"), "b"))
      ))
    }

    'queryTuple - {

      val q = for {
        a1 <- Query[TableA]
        a2 <- Query[TableA] if a1.a === a2.a
      } yield (a1.a, a2.b)

      implicitly[q.type <:< Q[(Ref[Int], Ref[String])]]

      val o = q.run

      implicitly[o.type <:< SelectResult[(Int, String)]]

      val sql = o.sql
      val expected = Select(
        fields = List(ExpressionField(Identifier(Some("foo"), "a"), None), ExpressionField(Identifier(Some("foo_1"), "b"), None)),
        from = List(TableName("foo", None), TableName("foo", Some("foo_1"))),
        where = Some(Equals(Identifier(Some("foo"), "a"), Identifier(Some("foo_1"), "a")))
      )
      assert(sql == expected)

      println(Generator.GenSelect.print(o.sql))
    }


    'queryTable - {
      val o = Query[TableA].run

      implicitly[o.type <:< SelectResult[TableA[Ctx.Concrete]]]

      val sql = o.sql
      val expected = Select(
        fields = List(ExpressionField(Identifier(Some("foo"), "a"), None), ExpressionField(Identifier(Some("foo"), "b"), None)),
        from =  List(TableName("foo", None))
      )
      assert(sql == expected)

//      println(Generator.GenSelect.print(o.sql))

    }


    'filter - {

      val q = for {
        ta <- Query[TableA]
        tb <- Query[TableB] if ta.a === tb.x
      } yield (ta.b, tb.y)

      val sql = q.run.sql

      val expected = Select(
        fields = List(ExpressionField(Identifier(Some("foo"), "b"), None), ExpressionField(Identifier(Some("bar"), "y"), None)),
        from =  List(TableName("foo", None), TableName("bar", None)),
        where = Some(Equals(Identifier(Some("foo"), "a"), Identifier(Some("bar"), "x")))
      )

      assert(sql == expected)

//      println(Generator.GenSelect.print(sql))

    }

  }


}
