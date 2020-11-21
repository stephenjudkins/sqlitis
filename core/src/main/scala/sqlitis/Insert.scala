package sqlitis

import sqlitis.Ctx.Queried
import sqlitis.Ctx
import sqlitis.Query.{Column, Table}
import shapeless._
import shapeless.ops.hlist.{FillWith, ZipApply}
import sqlitis.Sql.{Expression, Identifier, Literal}

import scala.reflect.ClassTag

object Insert {

  def value[T[_ <: Ctx]](_row: T[Ctx.Inserted]): Inserter[T, Long] =
    new Inserter[T, Long] {
      def row = _row
    }

  trait Inserter[T[_ <: Ctx], R] {
    def row: T[Ctx.Inserted]
    def run[F[_]](implicit build: BuildInsert[T, F, R]): F[R] = build.apply(row)
  }

  type RawSql[A] = Sql.Insert[Unit]

  trait BuildInsert[T[_ <: Ctx], F[_], R] {
    def apply(row: T[Ctx.Inserted]): F[R]
  }

  object BuildInsert {
    implicit def idInstance[T[_ <: Ctx], R, IH <: HList, SH <: HList, FH <: HList](implicit
        genI: Generic.Aux[T[Ctx.Inserted], IH],
        genS: Generic.Aux[T[Ctx.Schema], SH],
        genF: Generic.Aux[T[FetchCtx], FH],
        table: Table[T],
        insertHList: InsertHList[IH, SH, FH]
    ): BuildInsert[T, RawSql, R] =
      new BuildInsert[T, RawSql, R] {
        def apply(row: T[Ctx.Inserted]): Sql.Insert[Unit] = {

          val (names, values) = insertHList(genI.to(row), genS.to(table.schema)).unzip

          Sql.Insert(
            table.name,
            names,
            values
          )
        }
      }
  }

  trait InsertHList[IH <: HList, SH <: HList, FH <: HList] {
    def apply(ih: IH, sh: SH): List[(String, Expression[Unit])]
  }

  object InsertHList {
    implicit def insertWithDefault[I, IH <: HList, S <: Column[_], SH <: HList, FH <: HList](implicit
        insertTail: InsertHList[IH, SH, FH]
    ): InsertHList[Option[I] :: IH, S :: SH, HasDefaultMarker :: FH] =
      new InsertHList[Option[I] :: IH, S :: SH, HasDefaultMarker :: FH] {
        def apply(ih: Option[I] :: IH, sh: S :: SH) = {
          val maybeInsert = ih.head.map(_ => sh.head.name -> Literal(()))
          val tail        = insertTail(ih.tail, sh.tail)
          maybeInsert.map(_ :: tail).getOrElse(tail)
        }
      }

    implicit def insertWithoutDefault[I, IH <: HList, S <: Column[_], SH <: HList, FH <: HList](implicit
        insertTail: InsertHList[IH, SH, FH]
    ): InsertHList[I :: IH, S :: SH, NoDefaultMarker :: FH] =
      new InsertHList[I :: IH, S :: SH, NoDefaultMarker :: FH] {
        def apply(ih: I :: IH, sh: S :: SH) = {
          (sh.head.name -> Literal(())) :: insertTail(ih.tail, sh.tail)
        }
      }

    implicit def insertHNil: InsertHList[HNil, HNil, HNil] =
      new InsertHList[HNil, HNil, HNil] {
        def apply(ih: HNil, sh: HNil) = Nil
      }
  }

  type FetchCtx = Ctx {
    type NoDefault[X]  = NoDefaultMarker
    type HasDefault[X] = HasDefaultMarker
  }

  trait NoDefaultMarker
  trait HasDefaultMarker

}
