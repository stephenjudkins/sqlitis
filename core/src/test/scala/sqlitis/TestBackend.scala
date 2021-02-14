package sqlitis

import cats._
import sqlitis.Ctx.Inserted
import sqlitis.Insert.Convert
import sqlitis.util.{ReadFromReference, ResultExtractor}

trait UnitPut[A]

object UnitPut {
  def instance[A]: UnitPut[A] = new UnitPut[A] {}
}

trait NoGet[A]

object NoGet {
  private def instance[A]: NoGet[A] = new NoGet[A] {}

  implicit val apply: Applicative[NoGet] = new Applicative[NoGet] {
    def ap[A, B](ff: NoGet[A => B])(fa: NoGet[A]): NoGet[B] = instance[B]
    def pure[A](x: A): NoGet[A]                             = instance[A]
  }

  implicit def extract[A]: ReadFromReference[NoGet[A]] = new ReadFromReference[NoGet[A]] {
    def apply: NoGet[A] = instance[A]
  }
}

case class TestResult[A](sql: Sql.Statement[Unit])

object TestBackend extends Backend[UnitPut, Unit, NoGet, TestResult, TestResult[Unit]] {

  protected def elem[A: UnitPut](a: A): Unit = ()

  def select[A, O](q: Query[Unit, A])(implicit
      resultExtractor: ResultExtractor[A, O, Unit, NoGet]
  ): TestResult[O] = {
    val o = q.as[O, NoGet]

    TestResult[O](o.sql)
  }

  def insert[T[_ <: Ctx]: Query.Table, O](row: T[Inserted])(implicit
      b: Insert.BuildInsert[T, Unit, UnitPut, Unit]
  ): TestResult[Unit] =
    TestResult[Unit](
      b(
        row,
        new Convert[UnitPut, Unit] {
          def apply[A](a: A, put: UnitPut[A]): Unit = ()
        }
      )
    )

}
