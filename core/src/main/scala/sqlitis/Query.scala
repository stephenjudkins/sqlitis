package sqlitis

import shapeless._
import shapeless.ops.hlist.{Comapped, Mapper, ToTraversable, Tupler, ZipConst}
import sqlitis.Query.Ctx.{Concrete, Queried, Schema}
import sqlitis.Sql._

import scala.annotation.tailrec

object Query {

  trait Ctx {
    type NoDefault[_]
    type HasDefault[_]
  }

  object Ctx {
    type AllColumns[F[_]] = Ctx {
      type NoDefault[X] = F[X]
      type HasDefault[X] = F[X]
    }

    type Schema = AllColumns[Column]
    type Queried[X] = AllColumns[Ref[X, *]]
    type Concrete = AllColumns[Id]
    type Inserted = Ctx {
      type NoDefault[X] = X
      type HasDefault[X] = Option[X]
    }
  }


  case class Column[A](name: String)
  case class Ref[+X, A](expr: Expression[X]) {
    def ===[Y](other: Ref[Y, A])(implicit c: Ref[X, A] <:< Ref[Y, A]):Ref[Y, Boolean] = Ref[Y, Boolean](Equals[Y](c(this).expr, other.expr))
  }

  case class Extractor[A](i: Int)

  abstract class Table[T[_ <: Ctx]] {
    def schema: T[Schema]
    def name: String
  }

  private def relationName(current: Map[String, String], tableName: String):String = {
    @tailrec def findAlias(i: Int):String = {
      val p = s"${tableName}_$i"
      if (current.isDefinedAt(p)) findAlias(i + 1) else p
    }

    if (current.isDefinedAt(tableName)) findAlias(1) else tableName
  }


  def apply[T[_ <: Ctx]](implicit table: Table[T], querify: Querify[T]):Q[Unit, T[Queried[Unit]]] = new Q[Unit, T[Queried[Unit]]] {
    def apply(s: QueryState[Unit]): (T[Queried[Unit]], QueryState[Unit]) = {
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

  case class QueryState[+X](
    relations: Map[String, String],
    filter: Option[Expression[X]]
  )


  trait Querify[T[_ <: Ctx]] {
    def apply(tableAlias: String, t: T[Schema]):T[Queried[Unit]]
  }

  object Querify {
    def apply[T[_ <: Ctx]](tableAlias: String, t: T[Schema])(implicit q: Querify[T]):T[Queried[Unit]] = q(tableAlias, t)

    trait AssignAliasToTables[H0 <: HList, H1 <: HList] {
      def apply(tableName: String, h: H0):H1
    }

    implicit val assignAliasToTablesHNil:AssignAliasToTables[HNil, HNil] = new AssignAliasToTables[HNil, HNil] {
      def apply(tableName: String, h: HNil): HNil = HNil
    }

    implicit def assignAliasToTablesHCons[A, H0 <: HList,H1 <: HList](
      implicit tail: AssignAliasToTables[H0, H1]
    ):AssignAliasToTables[Column[A] :: H0, Ref[Unit, A] :: H1] =
      new AssignAliasToTables[Column[A] :: H0, Ref[Unit, A] :: H1] {
        def apply(tableName: String, h: Column[A] :: H0): Ref[Unit, A] :: H1 = {
          Ref[Unit, A](Identifier(Some(tableName), h.head.name)) :: tail(tableName, h.tail)
        }
      }

    implicit def instance[
      T[_ <: Ctx],
      H1 <: HList,
      H2 <: HList,
    ](
       implicit
       from: Generic.Aux[T[Schema], H1],
       querifier: AssignAliasToTables[H1, H2],
       to: Generic.Aux[T[Queried[Unit]], H2]
     ):Querify[T] = new Querify[T] {
      def apply(tableAlias: String, t: T[Schema]): T[Queried[Unit]] =
        to.from(querifier(tableAlias, from.to(t)))
    }

  }



  trait ResultExtractor[I, +O, X] {
    def apply(accum: List[Field[X]], a: I):List[Field[X]]
  }

  object ResultExtractor {


    implicit def hnilInstance[X]:ResultExtractor[HNil, HNil, X] = new ResultExtractor[HNil, HNil, X] {
      def apply(accum: List[Field[X]], a: HNil): List[Field[X]] = accum
    }

    implicit def hConsInstance[I, O, HI <: HList, HO <: HList, X](
      implicit h: ResultExtractor[I, O, X],
      tail: ResultExtractor[HI, HO, X]
    ): ResultExtractor[I :: HI,  O :: HO, X] =
      new ResultExtractor[I :: HI,  O :: HO, X] {
        def apply(accum: List[Field[X]], a: I :: HI): List[Field[X]] = h.apply(tail(accum, a.tail), a.head)
      }

    implicit def refInstance[A, X]:ResultExtractor[Ref[X, A], A, X] = new ResultExtractor[Ref[X, A], A, X]{
      def apply(accum: List[Field[X]], a: Ref[X, A]): List[Field[X]] = ExpressionField(a.expr, None) :: accum
    }

    implicit def tableInstance[T[_ <: Ctx], HI <: HList, HO <: HList, X](
      implicit
      from: Generic.Aux[T[Queried[X]], HI],
      to: Generic.Aux[T[Concrete], HO],
      gen: ResultExtractor[HI, HO, X]
    ):ResultExtractor[T[Queried[X]], T[Concrete], X] = new ResultExtractor[T[Queried[X]], T[Concrete], X] {
      def apply(accum: List[Field[X]], a: T[Queried[X]]): List[Field[X]] = gen(accum, from.to(a))
    }


    implicit def tupleInstance[I, HI <: HList, HO <: HList, O, X](
      implicit
      from: Generic.Aux[I, HI],
      gen: ResultExtractor[HI, HO, X],
      to: Tupler.Aux[HO, O],
      isTuple: Tupler.Aux[HI, I],
    ):ResultExtractor[I, O, X] = new ResultExtractor[I, O, X] {
      def apply(accum: List[Field[X]], a: I): List[Field[X]] = gen(accum, from.to(a))
    }

  }


  trait Q[X, A] { self =>
    def run[O](implicit resultExtractor: ResultExtractor[A, O, X]):SelectResult[X, O] = {

      val (result, queryState) = apply(QueryState(Map.empty, None))

      val select = Select(
        fields = resultExtractor(Nil, result),
        from = queryState.relations.toList.map { case (alias, t) => TableName(t, if (alias == t) None else Some(alias)) },
        where = queryState.filter
      )

      SelectResult[X, O](
        sql = select
      )
    }

    def apply(s: QueryState[X]):(A, QueryState[X])

    def flatMap[B](f: A => Q[X, B]):Q[X, B] = new Q[X, B] {
      def apply(s: QueryState[X]): (B, QueryState[X]) = {
        val (a, s2) = self(s)
        f(a).apply(s2)
      }
    }

    def map[B](f: A => B):Q[X, B] = flatMap(a => pure(f(a)))

    def withFilter(f: A => Ref[X, Boolean]):Q[X, A] = new Q[X, A] {
      def apply(s: QueryState[X]): (A, QueryState[X]) = {
        val (a, s2) = self(s)
        val filterExpr = f(a).expr
        val newFilter = s2.filter.map(And(_, filterExpr)).orElse(Some(filterExpr))
        (a, s2.copy(filter = newFilter))
      }
    }
  }

  def pure[X, A](a: A):Q[X, A] = new Q[X, A] {
    def apply(s: QueryState[X]): (A, QueryState[X]) = (a, s)
  }

  case class SelectResult[X, A](
    sql: Sql.Select[X]
  )

}


