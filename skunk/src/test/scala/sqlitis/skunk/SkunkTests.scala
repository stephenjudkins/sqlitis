package sqlitis.skunk

import cats._
import cats.implicits._
import cats.effect._
import skunk.{Codec, Session, Void}
import sqlitis.Ctx
import sqlitis.Query.{Column, Table}
import utest.{TestSuite, Tests, assert}
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

  def run[A](f: Session[IO] => QueryService[A]): List[A] = {
    val session: Resource[IO, Session[IO]] =
      Session.single[IO](
        host = "localhost",
        user = "stephen",
        database = "test"
      )

    session
      .use(s =>
        s.transaction.use { xa =>
          for {
            sp <- xa.savepoint
            _  <- schema.map(c => s.execute(_root_.skunk.Command(c, Origin.unknown, Void.codec))).sequence
            o  <- f(s).list
            _  <- xa.rollback(sp)
          } yield o
        }
      )
      .unsafeRunSync()
  }

  def tests: Tests = Tests {
    val query = from[Pet].filter(_.name === l("Hobbes")).map(t => (t.name, t.personId))

    val results = run(select(query))

    assert(results == List(("Hobbes", 2)))

  }
}
