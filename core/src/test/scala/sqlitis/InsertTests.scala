package sqlitis

import sqlitis.Query.{Column, Ctx, Table}
import utest._
import shapeless._
import sqlitis.Insert.BuildInsert
import sqlitis.Query.Ctx.Schema
import sqlitis.Sql.Literal

object InsertTests extends TestSuite {
  case class TableA[C <: Ctx](
    id: C#HasDefault[Int] = None,
    name: C#NoDefault[String]
  )

  implicit object TableA extends Table[TableA] {
    def schema: TableA[Schema] = TableA[Schema](
      id = Column("id"),
      name = Column("name")
    )

    def name: String = "table_a"
  }


  def tests = Tests {
    "insert" - {
      val sql = Insert.value(TableA[Ctx.Inserted](
        id = Some(42),
        name = "foobar"
      )).run[Insert.RawSql]

      val expected = Sql.Insert("table_a", List("id", "name"), List(Literal(()), Literal(())))
      assert(sql == expected)
    }

    "insert w/o default value" - {
      val sql = Insert.value(TableA[Ctx.Inserted](
        name = "foobar"
      )).run[Insert.RawSql]

      TableA[Schema](id = Column("id"), name = Column("name"))

      val expected = Sql.Insert("table_a", List( "name"), List(Literal(())))
      assert(sql == expected)
    }
  }
}
