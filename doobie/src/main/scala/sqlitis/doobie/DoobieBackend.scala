package sqlitis.doobie

import cats.{Applicative, Apply}
import doobie.util.fragment.{Elem, Fragment}
import doobie.util.query.Query0
import doobie.util.update.Update0
import doobie.util.{Get, Put, Read}
import sqlitis.Ctx.Inserted
import sqlitis.Insert.Convert
import sqlitis.Query.Ref
import sqlitis.Sql.Literal
import sqlitis.util.{ReadFromReference, ResultExtractor}
import sqlitis.{Backend, Ctx, Generator, Insert, Query}

trait ReadDoobie[A] {
  def read: Read[A]
}

object ReadDoobie {

  private def forRead[A](r: Read[A]): ReadDoobie[A] = new ReadDoobie[A] {
    def read: Read[A] = r
  }

  implicit val applicative: Applicative[ReadDoobie] = new Applicative[ReadDoobie] {
    def pure[A](x: A): ReadDoobie[A] = ReadDoobie.forRead(Read.unit.map(_ => x))
    def ap[A, B](ff: ReadDoobie[A => B])(fa: ReadDoobie[A]): ReadDoobie[B] =
      ReadDoobie.forRead(
        new Read(
          fa.read.gets ++ ff.read.gets,
          (rs, n) => ff.read.unsafeGet(rs, n)(fa.read.unsafeGet(rs, n + ff.read.length))
        )
      )

  }

  implicit def extractThis[A: Get]: ReadFromReference[ReadDoobie[A]] = new ReadFromReference[ReadDoobie[A]] {
    def apply: ReadDoobie[A] = forRead(Read.fromGet[A])
  }
}

object DoobieBackend extends Backend[Put, Elem, ReadDoobie, Query0, Update0] {

  def elem[A: Put](a: A): Elem = Elem.Arg(a, implicitly[Put[A]])

  def select[A, O](q: Query[Elem, A])(implicit resultExtractor: ResultExtractor[A, O, Elem, ReadDoobie]): Query0[O] = {
    val out         = q.as[O, ReadDoobie]
    val (args, sql) = Generator.GenSelect.generate(out.sql)

    Fragment(sql, args).query[O](out.decode.read)
  }

  def insert[T[_ <: Ctx]: Query.Table, O](
      row: T[Inserted]
  )(implicit b: Insert.BuildInsert[T, Elem, Put, Unit]): Update0 = {
    val i = b(
      row,
      new Convert[Put, Elem] {
        def apply[A](a: A, put: Put[A]): Elem = Elem.Arg(a, put)
      }
    )

    val (args, sql) = Generator.GenInsert.generate(i)

    Fragment(sql, args).update
  }
}
