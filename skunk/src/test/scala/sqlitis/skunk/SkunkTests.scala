package sqlitis.skunk

import cats._
import cats.implicits._
import cats.effect._
import skunk.{Codec, Session, Void}
import sqlitis.Ctx
import sqlitis.Query.{Column, Table}
import utest._
import natchez.Trace.Implicits.noop
import skunk.util.Origin

import scala.concurrent.ExecutionContext
import scala.io.Source

object SkunkTests extends TestSuite {
  protected implicit def contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def schema =
    Source
      .fromInputStream(getClass.getClassLoader.getResourceAsStream("test.sql"))
      .mkString
      .split(";")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  implicit val sCodec: Codec[String] = _root_.skunk.codec.all.text
  implicit val intCodec: Codec[Int]  = _root_.skunk.codec.all.int4

  case class Person[C <: Ctx](
      id: C#HasDefault[Int],
      name: C#NoDefault[String],
      age: C#NoDefault[Int]
  )

  implicit object Person extends Table[Person] {
    val name = "person"
    val schema = Person[Ctx.Schema](
      id = Column("id"),
      name = Column("name"),
      age = Column("age")
    )
  }

  case class Pet[C <: Ctx](
      id: C#HasDefault[Int],
      name: C#NoDefault[String],
      personId: C#NoDefault[Int]
  )

  implicit object Pet extends Table[Pet] {
    val name = "pet"
    val schema = Pet[Ctx.Schema](
      id = Column("id"),
      name = Column("name"),
      personId = Column("person_id")
    )
  }

  import SkunkBackend._

  private def session: Resource[IO, Session[IO]] =
    Session.single[IO](
      host = "localhost",
      user = "stephen",
      database = "test"
    )

  private def tx: Resource[IO, Session[IO]] =
    for {
      session     <- session
      transaction <- session.transaction
      _           <- Resource.make(transaction.savepoint)(transaction.rollback(_).as(()))
      _ <- Resource.liftF(
        schema.map(c => session.execute(_root_.skunk.Command(c, Origin.unknown, Void.codec))).sequence
      )
    } yield session

  def tests: Tests = Tests {
    "basic query" - {

      select(from[Pet].filter(_.name === l("Hobbes")).map(p => (p.name, p.personId)))

      val results =
        tx
          .map(select(from[Pet].filter(_.name === l("Hobbes")).map(p => (p.name, p.personId))))
          .use(_.list)
          .unsafeRunSync()

      assert(results == List(("Hobbes", 2)))
    }

    "insert" - {
      val i = insert(
        Person[Ctx.Inserted](
          id = Some(42),
          name = "Zed",
          age = 100
        )
      )

      val s = select(from[Person].filter(_.id === l(42)).map(_.name))

      val results = tx
        .use(session =>
          for {
            _ <- i(session)
            r <- s(session).list
          } yield r
        )
        .unsafeRunSync()

      assert(results == List("Zed"))
    }
  }
}
