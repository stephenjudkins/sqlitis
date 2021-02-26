package sqlitis.skunk

import cats.{Applicative, Apply}
import cats.data.State
import cats.effect.IO
import skunk._
import skunk.data.Type
import skunk.util.Origin
import sqlitis.Ctx.Inserted
import sqlitis.Insert.Convert
import sqlitis.skunk.QueryService.{Q, Update}
import sqlitis.{Backend, Ctx, Generator, Insert, Query}
import sqlitis.util.{ReadFromReference, ResultExtractor}

trait QueryService[A] {
  def list(session: Session[IO]): IO[List[A]]
}

object QueryService {
  type Q[A]   = QueryService[A]
  type Update = Session[IO] => IO[Unit]
}

case class Encoded[A](a: A, encoder: Encoder[A]) {
  def encoded = encoder.encode(a)
}

object SkunkBackend extends Backend[Encoder, Encoded[_], Decoder, QueryService.Q, QueryService.Update] {

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

    val (args, sql) = Generator.GenSelect.generate(out.sql, i => s"$$$i")

    val e: Encoder[List[Encoded[_]]] = new Encoder[List[Encoded[_]]] {
      def sql: State[Int, String]                           = State.empty
      def encode(a: List[Encoded[_]]): List[Option[String]] = a.flatMap(_.encoded)
      def types: List[Type]                                 = args.flatMap(_.encoder.types)
    }

    val query = skunk.Query[List[Encoded[_]], O](sql, Origin.unknown, e, out.decode)

    new QueryService[O] {
      def list(session: Session[IO]): IO[List[O]] = session.prepare(query).use(p => p.stream(args, 42).compile.toList)
    }

  }

  def insert[T[_ <: Ctx]: Query.Table, O](
      row: T[Inserted]
  )(implicit b: Insert.BuildInsert[T, Encoded[_], Encoder, Unit]): Update = {
    val insert = b(
      row,
      new Convert[Encoder, Encoded[_]] {
        def apply[A](a: A, put: Encoder[A]): Encoded[_] = Encoded(a, put)
      }
    )

    val (args, sql) = Generator.GenInsert.generate(insert, i => s"$$$i")

    val e: Encoder[List[Encoded[_]]] = new Encoder[List[Encoded[_]]] {
      def sql: State[Int, String]                           = State.empty
      def encode(a: List[Encoded[_]]): List[Option[String]] = a.flatMap(_.encoded)
      def types: List[Type]                                 = args.flatMap(_.encoder.types)
    }

    val command = skunk.Command[List[Encoded[_]]](sql, Origin.unknown, e)

    { session: Session[IO] =>
      session.prepare(command).use(c => c.execute(args)).as(())
    }
  }
}
