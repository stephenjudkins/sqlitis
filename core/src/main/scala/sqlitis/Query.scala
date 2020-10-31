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
    type Queried = AllColumns[Ref]
    type Concrete = AllColumns[Id]
    type Inserted = Ctx {
      type NoDefault[X] = X
      type HasDefault[X] = Option[X]
    }
  }


  case class Column[A](name: String)
  case class Ref[A](expr: Expression[Unit]) {
    def ===(other: Ref[A]) = Ref[Boolean](Equals(expr, other.expr))
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


  def apply[T[_ <: Ctx]](implicit table: Table[T], querify: Querify[T]):Q[T[Queried]] = new Q[T[Queried]] {
    def apply(s: QueryState): (T[Queried], QueryState) = {
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

  case class QueryState(
    relations: Map[String, String],
    filter: Option[Expression[Unit]]
  )


  trait Querify[T[_ <: Ctx]] {
    def apply(tableAlias: String, t: T[Schema]):T[Queried]
  }

  object Querify {
    def apply[T[_ <: Ctx]](tableAlias: String, t: T[Schema])(implicit q: Querify[T]):T[Queried] = q(tableAlias, t)

    object M extends Poly1 {
      implicit def c[A]:Case.Aux[(Column[A], String), Ref[A]] = at[(Column[A], String)] {
        case (c, tbl) => Ref[A](Identifier(Some(tbl), c.name))
      }
    }

    implicit def instance[
      T[_ <: Ctx],
      H1 <: HList,
      H2 <: HList,
      H3 <: HList
    ](
      implicit
      from: Generic.Aux[T[Schema], H1],
      zipper: ZipConst.Aux[String, H1, H2],
      mapper: Mapper.Aux[M.type, H2, H3],
      to: Generic.Aux[T[Queried], H3]
    ):Querify[T] = new Querify[T] {
      def apply(tableAlias: String, t: T[Schema]): T[Queried] = to.from(mapper(zipper(tableAlias, from.to(t))))
    }
  }



  trait ResultExtractor[I, O] {
    def apply(a: I):List[Field[Unit]]
  }
  object ResultExtractor {

    private def toFields(l: List[Ref[_]]) = l.map {
      case Ref(expr) => ExpressionField(expr, None)
    }

    implicit def tableInstance[T[_ <: Ctx], HI <: HList, HO <: HList](
      implicit
      g1: Generic.Aux[T[Queried], HI],
      comapped: Comapped.Aux[HI, Ref, HO],
      toList: ToTraversable.Aux[HI, List, Ref[_]]
    ):ResultExtractor[T[Queried], T[Concrete]] = new ResultExtractor[T[Queried], T[Concrete]] {
      def apply(a: T[Queried]): List[Field[Unit]] = toFields(toList(g1.to(a)))
    }

    implicit def tupleInstance[I, O, HI <: HList, HO <: HList](
      implicit
      g1: Generic.Aux[I, HI],
      tIn: Tupler.Aux[HI, I],
      comapped: Comapped.Aux[HI, Ref, HO],
      tOut: Tupler.Aux[HO, O],
      toList: ToTraversable.Aux[HI, List, Ref[_]]
    ):ResultExtractor[I,O] = new ResultExtractor[I, O] {
      def apply(a: I): List[Field[Unit]] = toFields(toList(g1.to(a)))
    }
  }


  trait Q[A] { self =>
    def run[O](implicit resultExtractor: ResultExtractor[A, O]):SelectResult[O] = {

      val (result, queryState) = apply(QueryState(Map.empty, None))

      val select = Select(
        fields = resultExtractor(result),
        from = queryState.relations.toList.map { case (alias, t) => TableName(t, if (alias == t) None else Some(alias)) },
        where = queryState.filter
      )

      SelectResult[O](
        sql = select
      )
    }

    def apply(s: QueryState):(A, QueryState)

    def flatMap[B](f: A => Q[B]):Q[B] = new Q[B] {
      def apply(s: QueryState): (B, QueryState) = {
        val (a, s2) = self(s)
        f(a).apply(s2)
      }
    }

    def map[B](f: A => B) = flatMap(a => pure(f(a)))

    def withFilter(f: A => Ref[Boolean]):Q[A] = new Q[A] {
      def apply(s: QueryState): (A, QueryState) = {
        val (a, s2) = self(s)
        val filterExpr = f(a).expr
        val newFilter = s2.filter.map(And(_, filterExpr)).orElse(Some(filterExpr))
        (a, s2.copy(filter = newFilter))
      }
    }
  }

  def pure[A](a: A):Q[A] = new Q[A] {
    def apply(s: QueryState): (A, QueryState) = (a, s)
  }





  case class SelectResult[A](
    sql: Sql.Select[Unit]
  )

}


