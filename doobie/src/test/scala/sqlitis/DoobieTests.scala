package sqlitis
import utest._
import cats.effect._
import cats.implicits._
import _root_.doobie.{Query => _, _}
import _root_.doobie.implicits._
import _root_.doobie.h2._
import sqlitis.Query.{Column, Table}
import doobie.DoobieBackend
import shapeless._

import scala.io.Source
import scala.concurrent.ExecutionContext

object DoobieTests extends TestSuite {
  protected implicit def contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def schema = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("test.sql")).mkString

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

  private def run[A](c: ConnectionIO[A]): A = {

    val transactor: Resource[IO, H2Transactor[IO]] =
      for {
        ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
        te <- ExecutionContexts.cachedThreadPool[IO] // our transaction EC
        xa <- H2Transactor.newH2Transactor[IO](
          s"jdbc:h2:mem:;DB_CLOSE_DELAY=-1", // connect URL
          "sa",                              // username
          "",                                //   password
          ce,                                // await connection here
          Blocker.liftExecutionContext(te)   // execute JDBC operations here
        )
      } yield xa

    transactor
      .use { xa =>
        for {
          _ <- Update(schema).run(()).transact(xa)
          r <- c.transact(xa)
        } yield r
      }
      .unsafeRunSync()
  }

  import DoobieBackend._

  def tests =
    Tests {
      "basicQuery" - {
        val query = for {
          person <- from[Person]
          pet    <- from[Pet] if person.id === pet.personId
        } yield (person.name, pet.name)

        val results = run(select(query).to[List])
        assert(
          results == List(
            ("Charlie Brown", "Snoopy"),
            ("Calvin", "Hobbes")
          )
        )
      }

      "parameterizedQuery" - {
        val query = from[Pet].filter(_.name === l("Hobbes")).map(_.name)

        val results = run(select(query).to[List])

        assert(results == List("Hobbes"))
      }
    }
}
