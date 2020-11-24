package sqlitis

import cats._
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

case class TestResult[A](sql: Sql.Select[Unit])

object TestBackend extends Backend[UnitPut, Unit, NoGet, TestResult] {

  protected def elem[A: UnitPut](a: A): Unit = ()

  def select[A, O](q: Query[Unit, A])(implicit
      resultExtractor: ResultExtractor[A, O, Unit, NoGet]
  ): TestResult[O] = {
    val o = q.as[O, NoGet]

    TestResult[O](o.sql)
  }

}
