package sqlitis

import cats.implicits._
import cats.{Applicative, Apply}
import shapeless._
import shapeless.ops.hlist.Tupler
import sqlitis.Ctx.{Concrete, Queried, Schema}
import sqlitis.Sql._
import sqlitis.util.{ResultExtractor, TableToQuery}

import scala.annotation.tailrec

object Query {

  case class Column[A](name: String)
  case class Ref[X, A](expr: Expression[X]) {
    def ===(other: Ref[X, A]): Ref[X, Boolean] =
      Ref[X, Boolean](Equals[X](expr, other.expr))
  }

  abstract class Table[T[_ <: Ctx]] {
    def schema: T[Schema]
    def name: String
  }

  private def relationName(current: Map[String, String], tableName: String): String = {
    @tailrec def findAlias(i: Int): String = {
      val p = s"${tableName}_$i"
      if (current.isDefinedAt(p)) findAlias(i + 1) else p
    }

    if (current.isDefinedAt(tableName)) findAlias(1) else tableName
  }

  def table[X, T[_ <: Ctx]](implicit table: Table[T], querify: TableToQuery[X, T]): Query[X, T[Queried[X]]] =
    new Query[X, T[Queried[X]]] {
      def apply(s: QueryState[X]): (T[Queried[X]], QueryState[X]) = {
        val tableName = table.name
        val aliasName = relationName(s.relations, tableName)
        (
          querify(aliasName, table.schema),
          s.copy(
            relations = s.relations + (aliasName -> tableName)
          )
        )
      }
    }

  def apply[T[_ <: Ctx]: Table](implicit querify: TableToQuery[Unit, T]): Query[Unit, T[Queried[Unit]]] =
    table[Unit, T]

  case class QueryState[+X](
      relations: Map[String, String],
      filter: Option[Expression[X]]
  )

  def pure[X, A](a: A): Query[X, A] =
    new Query[X, A] {
      def apply(s: QueryState[X]): (A, QueryState[X]) = (a, s)
    }

  case class SelectResult[X, A, R[_]](
      sql: Sql.Select[X],
      result: R[A]
  )

}

import Query._

trait Query[X, A] { self =>

  def as[O, R[_]](implicit resultExtractor: ResultExtractor[A, O, X, R]): SelectResult[X, O, R] = {

    val (applied, queryState) = apply(QueryState(Map.empty, None))

    val (fields, result) = resultExtractor(Nil, applied)

    val select = Select(
      fields = fields,
      from = queryState.relations.toList.map { case (alias, t) =>
        TableName(t, if (alias == t) None else Some(alias))
      },
      where = queryState.filter
    )

    SelectResult[X, O, R](
      sql = select,
      result = result
    )
  }

  def apply(s: QueryState[X]): (A, QueryState[X])

  def flatMap[B](f: A => Query[X, B]): Query[X, B] =
    new Query[X, B] {
      def apply(s: QueryState[X]): (B, QueryState[X]) = {
        val (a, s2) = self(s)
        f(a).apply(s2)
      }
    }

  def map[B](f: A => B): Query[X, B] = flatMap(a => pure(f(a)))

  def withFilter(f: A => Ref[X, Boolean]): Query[X, A] =
    new Query[X, A] {
      def apply(s: QueryState[X]): (A, QueryState[X]) = {
        val (a, s2)    = self(s)
        val filterExpr = f(a).expr
        val newFilter  = s2.filter.map(And(_, filterExpr)).orElse(Some(filterExpr))
        (a, s2.copy(filter = newFilter))
      }
    }
}
