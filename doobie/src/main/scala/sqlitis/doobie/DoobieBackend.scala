package sqlitis.doobie

import cats.{Applicative, Apply}
import doobie.util.fragment.{Elem, Fragment}
import doobie.util.query.Query0
import doobie.util.{Get, Put, Read}
import sqlitis.util.{ReadFromReference, ResultExtractor}
import sqlitis.{Backend, Generator, Query}

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
      ReadDoobie.forRead(Apply[Read].ap(ff.read)(fa.read))
  }

  implicit def extractThis[A: Get]: ReadFromReference[ReadDoobie[A]] = new ReadFromReference[ReadDoobie[A]] {
    def apply: ReadDoobie[A] = forRead(Read.fromGet[A])
  }
}

object DoobieBackend extends Backend[Put, Elem, ReadDoobie, Query0] {

  def l[A: Put](a: A): Elem = Elem.Arg(a, implicitly[Put[A]])

  def select[A, O](q: Query[Elem, A])(implicit resultExtractor: ResultExtractor[A, O, Elem, ReadDoobie]): Query0[O] = {
    val out         = q.as[O, ReadDoobie]
    val (args, sql) = Generator.GenSelect.generate(out.sql)

    implicit val r: Read[O] = out.result.read

    Fragment(sql, args).query[O]
  }
}
