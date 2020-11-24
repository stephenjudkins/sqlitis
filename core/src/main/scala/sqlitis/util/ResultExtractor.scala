package sqlitis.util

import cats._
import cats.implicits._
import shapeless._
import shapeless.ops.hlist.Tupler
import sqlitis.Ctx.{Concrete, Queried}
import sqlitis.Ctx
import sqlitis.Query.Ref
import sqlitis.Sql.{ExpressionField, Field}

trait ResultExtractor[I, O, X, R[_]] {
  def apply(accum: List[Field[X]], a: I): (List[Field[X]], R[O])
}

object ResultExtractor {

  implicit def hnilInstance[X, R[_]: Applicative]: ResultExtractor[HNil, HNil, X, R] =
    new ResultExtractor[HNil, HNil, X, R] {
      def apply(accum: List[Field[X]], a: HNil) = (accum, Applicative[R].pure(HNil))
    }

  implicit def hConsInstance[I, O, HI <: HList, HO <: HList, X, R[_]: Apply](implicit
      h: ResultExtractor[I, O, X, R],
      tail: ResultExtractor[HI, HO, X, R]
  ): ResultExtractor[I :: HI, O :: HO, X, R] =
    new ResultExtractor[I :: HI, O :: HO, X, R] {
      def apply(accum: List[Field[X]], a: I :: HI) = {

        val (accum1, rt) = tail(accum, a.tail)
        val (accum2, rh) = h(accum1, a.head)

        (accum2, (rh, rt).mapN { case (h, t) => h :: t })
      }
    }

  implicit def refInstance[A, X, R[_]](implicit
      extract: ReadFromReference[R[A]]
  ): ResultExtractor[Ref[X, A], A, X, R] =
    new ResultExtractor[Ref[X, A], A, X, R] {
      def apply(accum: List[Field[X]], a: Ref[X, A]) = {
        val fields = ExpressionField(a.expr, None) :: accum

        (fields, extract.apply)
      }
    }

  implicit def tableInstance[T[_ <: Ctx], HI <: HList, HO <: HList, X, R[_]: Applicative](implicit
      from: Generic.Aux[T[Queried[X]], HI],
      to: Generic.Aux[T[Concrete], HO],
      gen: ResultExtractor[HI, HO, X, R]
  ): ResultExtractor[T[Queried[X]], T[Concrete], X, R] =
    new ResultExtractor[T[Queried[X]], T[Concrete], X, R] {
      def apply(accum: List[Field[X]], a: T[Queried[X]]) = {
        val (fields, ho) = gen(accum, from.to(a))

        (fields, ho.map(to.from))
      }
    }

  implicit def tupleInstance[I, HI <: HList, HO <: HList, O, X, R[_]: Applicative](implicit
      from: Generic.Aux[I, HI],
      gen: ResultExtractor[HI, HO, X, R],
      to: Tupler.Aux[HO, O],
      isTuple: Tupler.Aux[HI, I]
  ): ResultExtractor[I, O, X, R] =
    new ResultExtractor[I, O, X, R] {
      def apply(accum: List[Field[X]], a: I) = {
        val (fields, ho) = gen(accum, from.to(a))

        (fields, ho.map(to.apply))
      }
    }

}
