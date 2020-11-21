package sqlitis.util

import shapeless._
import sqlitis.Ctx.{Queried, Schema}
import sqlitis.Ctx
import sqlitis.Query.{Column, Ref}
import sqlitis.Sql.Identifier

trait TableToQuery[X, T[_ <: Ctx]] {
  def apply(tableAlias: String, t: T[Schema]): T[Queried[X]]
}

object TableToQuery {
  def apply[X, T[_ <: Ctx]](tableAlias: String, t: T[Schema])(implicit q: TableToQuery[X, T]): T[Queried[X]] =
    q(tableAlias, t)

  trait AssignAliasToTables[X, H0 <: HList, H1 <: HList] {
    def apply(tableName: String, h: H0): H1
  }

  implicit def assignAliasToTablesHNil[X]: AssignAliasToTables[X, HNil, HNil] =
    new AssignAliasToTables[X, HNil, HNil] {
      def apply(tableName: String, h: HNil): HNil = HNil
    }

  implicit def assignAliasToTablesHCons[X, A, H0 <: HList, H1 <: HList](implicit
      tail: AssignAliasToTables[X, H0, H1]
  ): AssignAliasToTables[X, Column[A] :: H0, Ref[X, A] :: H1] =
    new AssignAliasToTables[X, Column[A] :: H0, Ref[X, A] :: H1] {
      def apply(tableName: String, h: Column[A] :: H0): Ref[X, A] :: H1 =
        Ref[X, A](Identifier(Some(tableName), h.head.name)) :: tail(tableName, h.tail)
    }

  implicit def instance[
      T[_ <: Ctx],
      H1 <: HList,
      H2 <: HList,
      X
  ](implicit
      from: Generic.Aux[T[Schema], H1],
      querifier: AssignAliasToTables[X, H1, H2],
      to: Generic.Aux[T[Queried[X]], H2]
  ): TableToQuery[X, T] =
    new TableToQuery[X, T] {
      def apply(tableAlias: String, t: T[Schema]): T[Queried[X]] =
        to.from(querifier(tableAlias, from.to(t)))
    }

}
