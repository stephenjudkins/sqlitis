package sqlitis

import sqlitis.Ctx.{Inserted, Queried}
import sqlitis.Ctx
import sqlitis.Query.{Column, Table}
import shapeless._
import shapeless.ops.hlist.{FillWith, ZipApply}
import sqlitis.Sql.{Expression, Identifier, Literal}

import scala.reflect.ClassTag

object Insert {

  trait NoPut[A]
  object NoPut {
    implicit def instance[A]: NoPut[A] = new NoPut[A] {}
  }

  def value[T[_ <: Ctx]](_row: T[Ctx.Inserted]): Inserter[T, Long] =
    new Inserter[T, Long] {
      def row = _row
    }

  trait Inserter[T[_ <: Ctx], R] {
    def row: T[Ctx.Inserted]
    def run(implicit build: BuildInsert[T, Unit, NoPut, R]): Sql.Insert[Unit] = build.apply(
      row,
      new Convert[NoPut, Unit] {
        def apply[A](a: A, put: NoPut[A]): Unit = ()
      }
    )
  }

  trait Convert[Put[_], Elem] {
    def apply[A](a: A, put: Put[A]): Elem
  }

  trait BuildInsert[T[_ <: Ctx], Elem, Put[_], R] {
    def apply(row: T[Ctx.Inserted], convert: Convert[Put, Elem]): Sql.Insert[Elem]
  }

  object BuildInsert {
    implicit def idInstance[T[_ <: Ctx], R, IH <: HList, SH <: HList, FH <: HList, E, Put[_]](implicit
        genI: Generic.Aux[T[Ctx.Inserted], IH],
        genS: Generic.Aux[T[Ctx.Schema], SH],
        genF: Generic.Aux[T[FetchCtx], FH],
        table: Table[T],
        insertHList: InsertHList[IH, SH, FH, E, Put]
    ): BuildInsert[T, E, Put, R] =
      new BuildInsert[T, E, Put, R] {

        def apply(row: T[Inserted], convert: Convert[Put, E]): Sql.Insert[E] = {

          val (names, values) = insertHList(genI.to(row), genS.to(table.schema), convert).unzip

          Sql.Insert(
            table.name,
            names,
            values
          )
        }
      }
  }

  trait InsertHList[IH <: HList, SH <: HList, FH <: HList, E, Put[_]] {
    def apply(ih: IH, sh: SH, convert: Convert[Put, E]): List[(String, Expression[E])]
  }

  object InsertHList {
    implicit def insertWithDefault[I, IH <: HList, S <: Column[_], SH <: HList, FH <: HList, E, Put[_]](implicit
        insertTail: InsertHList[IH, SH, FH, E, Put],
        put: Put[I]
    ): InsertHList[Option[I] :: IH, S :: SH, HasDefaultMarker :: FH, E, Put] =
      new InsertHList[Option[I] :: IH, S :: SH, HasDefaultMarker :: FH, E, Put] {
        def apply(ih: Option[I] :: IH, sh: S :: SH, convert: Convert[Put, E]) = {
          val maybeInsert = ih.head.map(v => sh.head.name -> Literal(convert(v, put)))
          val tail        = insertTail(ih.tail, sh.tail, convert)
          maybeInsert.map(_ :: tail).getOrElse(tail)
        }
      }

    implicit def insertWithoutDefault[I, IH <: HList, S <: Column[_], SH <: HList, FH <: HList, E, Put[_]](implicit
        insertTail: InsertHList[IH, SH, FH, E, Put],
        put: Put[I]
    ): InsertHList[I :: IH, S :: SH, NoDefaultMarker :: FH, E, Put] =
      new InsertHList[I :: IH, S :: SH, NoDefaultMarker :: FH, E, Put] {
        def apply(ih: I :: IH, sh: S :: SH, convert: Convert[Put, E]) = {
          (sh.head.name -> Literal(convert(ih.head, put))) :: insertTail(ih.tail, sh.tail, convert)
        }
      }

    implicit def insertHNil[E, Put[_]]: InsertHList[HNil, HNil, HNil, E, Put] =
      new InsertHList[HNil, HNil, HNil, E, Put] {
        def apply(ih: HNil, sh: HNil, convert: Convert[Put, E]): List[(String, Expression[E])] = Nil

      }
  }

  type FetchCtx = Ctx {
    type NoDefault[X]  = NoDefaultMarker
    type HasDefault[X] = HasDefaultMarker
  }

  trait NoDefaultMarker
  trait HasDefaultMarker

}
