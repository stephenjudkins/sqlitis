package sqlitis.skunk

import cats.{Applicative, Apply}
import cats.data.State
import cats.effect.IO
import skunk._
import skunk.data.Type
import skunk.util.Origin
import sqlitis.skunk.QueryService.Q
import sqlitis.{Backend, Generator, Query}
import sqlitis.util.{ReadFromReference, ResultExtractor}

trait QueryService[A] {
  def list: IO[List[A]]
}

object QueryService {
  type Q[A] = Session[IO] => QueryService[A]
}

case class Encoded[A](a: A, encoder: Encoder[A]) {
  def encoded = encoder.encode(a)
}

object SkunkBackend extends Backend[Encoder, Encoded[_], Decoder, QueryService.Q] {

  implicit val encoderApplicative: Applicative[Decoder] = new Applicative[Decoder] {
    def pure[A](x: A): Decoder[A] = new Decoder[A] {
      def types: List[Type]                                                       = Nil
      def decode(offset: Int, ss: List[Option[String]]): Either[Decoder.Error, A] = Right(x)
    }
    def ap[A, B](ff: Decoder[A => B])(fa: Decoder[A]): Decoder[B] = Decoder.ApplyDecoder.ap(ff)(fa)
  }

  implicit def rfr[A: Decoder]: ReadFromReference[Decoder[A]] = new ReadFromReference[Decoder[A]] {
    def apply: Decoder[A] = implicitly[Decoder[A]]
  }

  protected def elem[A: Encoder](a: A): Encoded[_] = Encoded[A](a, implicitly)

  def select[A, O](q: sqlitis.Query[Encoded[_], A])(implicit
      resultExtractor: ResultExtractor[A, O, Encoded[_], Decoder]
  ) = {
    val out = q.as[O, Decoder]
    // gotta reverse these because Skunk's `Apply` is right-associative
    val (args, sql) = Generator.GenSelect.generate(out.sql.reverseFields, i => s"$$$i")

    val e: Encoder[List[Encoded[_]]] = new Encoder[List[Encoded[_]]] {
      def sql: State[Int, String]                           = State.empty
      def encode(a: List[Encoded[_]]): List[Option[String]] = a.flatMap(_.encoded)
      def types: List[Type]                                 = args.flatMap(_.encoder.types)
    }

    val query = skunk.Query[List[Encoded[_]], O](sql, Origin.unknown, e, out.decode)

    { session: Session[IO] =>
      new QueryService[O] {
        def list: IO[List[O]] = session.prepare(query).use(p => p.stream(args, 42).compile.toList)
      }
    }

  }

}
