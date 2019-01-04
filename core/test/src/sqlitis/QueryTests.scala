package sqlitis

import shapeless.Id
import sqlitis.Query._
import sqlitis.Sql._
import utest._

object QueryTests extends TestSuite {


  case class TableA[F[_]](
    a: F[Int],
    b: F[String]
  ) extends Table[F] {
    val tableName = "foo"
  }

  val tableA = TableA[Column](
    a = Column[Int]("a"),
    b = Column[String]("b")
  )

  case class TableB[F[_]](
    x: F[Int],
    y: F[String]
  ) extends Table[F] {
    val tableName = "bar"
  }

  val tableB = TableB[Column](
    x = Column[Int]("x"),
    y = Column[String]("y")
  )




  def tests: Tests = Tests {
    'Querify - {
      val q = Querify("foo_42", tableA)

      assert(q == TableA[Ref](
        a = Ref[Int](Identifier(Some("foo_42"), "a")),
        b = Ref[String](Identifier(Some("foo_42"), "b"))
      ))
    }

    'queryTuple - {

      val q = for {
        a1 <- Query.query(tableA)
        a2 <- Query.query(tableA)
      } yield (a1.a, a2.b)

      implicitly[q.type <:< Q[(Ref[Int], Ref[String])]]

      val o = q.run

      implicitly[o.type <:< SelectResult[(Int, String)]]

      val sql = o.sql
      val expected = Select(
        fields = List(ExpressionField(Identifier(Some("foo"), "a"), None), ExpressionField(Identifier(Some("foo_1"), "b"), None)),
        from =  List(TableName("foo", None), TableName("foo", Some("foo_1")))
      )
      assert(sql == expected)

      println(Generator.GenSelect.print(o.sql))
    }


    'queryTable - {
      val o = Query.query(tableA).run

      implicitly[o.type <:< SelectResult[TableA[Id]]]

      val sql = o.sql
      val expected = Select(
        fields = List(ExpressionField(Identifier(Some("foo"), "a"), None), ExpressionField(Identifier(Some("foo"), "b"), None)),
        from =  List(TableName("foo", None))
      )
      assert(sql == expected)

      println(Generator.GenSelect.print(o.sql))

    }


    'filter - {

      val q = for {
        ta <- Query.query(tableA)
        tb <- Query.query(tableB) if ta.a === tb.x
      } yield (ta.b, tb.y)

      val sql = q.run.sql

      val expected = Select(
        fields = List(ExpressionField(Identifier(Some("foo"), "b"), None), ExpressionField(Identifier(Some("bar"), "y"), None)),
        from =  List(TableName("foo", None), TableName("bar", None)),
        where = Some(Equals(Identifier(Some("foo"), "a"), Identifier(Some("bar"), "x")))
      )

      assert(sql == expected)

      println(Generator.GenSelect.print(sql))

    }



  }


}
