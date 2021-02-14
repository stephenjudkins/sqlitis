package sqlitis

import sqlitis.Ctx.Queried
import sqlitis.Insert.BuildInsert
import sqlitis.Query
import sqlitis.Query.{Ref, Table}
import sqlitis.Sql.Literal
import sqlitis.util.{ResultExtractor, TableToQuery}

trait Backend[Put[_], Elem, Get[_], Result[_], UpdateResult] {

  def l[A: Put](a: A): Query.Ref[Elem, A] = Ref[Elem, A](Literal(elem(a)))

  protected def elem[A: Put](a: A): Elem

  def from[T[_ <: Ctx]: Table](implicit querify: TableToQuery[Elem, T]): Query[Elem, T[Queried[Elem]]] =
    Query.table[Elem, T]

  def select[A, O](q: Query[Elem, A])(implicit
      resultExtractor: ResultExtractor[A, O, Elem, Get]
  ): Result[O]

  def insert[T[_ <: Ctx]: Table, O](row: T[Ctx.Inserted])(implicit b: BuildInsert[T, Elem, Put, Unit]): UpdateResult
}
